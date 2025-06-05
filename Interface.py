#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import os

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='ubuntu', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    cur.execute("create table " + ratingstablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
    cur.execute("alter table " + ratingstablename + " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;")
    cur.close()
    con.commit()

def loadratings1(ratingstablename, ratingsfilepath, openconnection):
    try:
        cur = openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
        cur.execute("""
            CREATE TABLE """ + ratingstablename + """ (
                UserID INTEGER,
                MovieID INTEGER,
                Rating FLOAT
            )
        """)

        # Tạo file tạm thời với định dạng phù hợp
        temp_file = 'temp_ratings.dat'
        with open(ratingsfilepath, 'r', encoding='utf-8') as infile, open(temp_file, 'w', encoding='utf-8') as outfile:
            for line in infile:
                parts = line.strip().split('::')
                if len(parts) == 4:
                    outfile.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")

        # Sử dụng copy_from để nạp dữ liệu
        with open(temp_file, 'r', encoding='utf-8') as f:
            cur.copy_from(f, ratingstablename, sep='\t', null='')
        openconnection.commit()
        print("Data loaded successfully into " + ratingstablename)

        # Xóa file tạm thời
        os.remove(temp_file)
    except psycopg2.Error as e:
        print("Error loading ratings: " + str(e))
        openconnection.rollback()
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    for i in range(0, numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        if i == 0:
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(minRange) + " and rating <= " + str(maxRange) + ";")
        else:
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(minRange) + " and rating <= " + str(maxRange) + ";")
    cur.close()
    con.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, ROW_NUMBER() over() as rnum from " + ratingstablename + ") as temp where mod(temp.rnum-1, 5) = " + str(i) + ";")
    cur.close()
    con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("select count(*) from " + ratingstablename + ";");
    total_rows = (cur.fetchall())[0][0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows-1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

def create_db(dbname):
    try:
        con = getopenconnection(dbname='postgres')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute('CREATE DATABASE %s' % (dbname,))
        else:
            print('A database named {0} already exists'.format(dbname))
        cur.close()
        con.close()
    except psycopg2.Error as e:
        print("Error creating database: " + str(e))

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
