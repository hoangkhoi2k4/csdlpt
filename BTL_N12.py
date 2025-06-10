import time
import psycopg2
import os

def loadratings(ratingstablename, ratingsfilepath, openconnection):
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
    Based on range of ratings, create new partitions from main table (ratings)
    """
    try:
        cur = openconnection.cursor()

        # Tính phạm vi rating cho mỗi mảnh
        d = 5 / numberofpartitions

        # Với mỗi mảnh thứ i tạo bảng mới có tiền tố range_part + i, 
        # Lấy dữ liệu từ bảng rating gốc thêm vào các mảnh
        for i in range(numberofpartitions):
            tb_name = f'range_part{i}'
            min_rate = i * d
            max_rate = min_rate + d
            if i == 0:
                cur.execute(
                    f'CREATE TABLE {tb_name} AS '
                    f'SELECT userid, movieid, rating FROM {ratingstablename} '
                    f'WHERE rating <= {max_rate};'
                )
            else:
                cur.execute(
                    f'CREATE TABLE {tb_name} AS '
                    f'SELECT userid, movieid, rating FROM {ratingstablename} '
                    f'WHERE rating > {min_rate} and rating <= {max_rate};'
                )
        cur.close()
        openconnection.commit()
    except Exception as ex: 
        openconnection.rollback()
        print(f'Phân mảng ngang theo khoảng thất bại: {str(ex)}')

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    try:
        cur = openconnection.cursor()

        # Tìm số lượng mảnh thông qua đếm số lượng bảng có tiền tố là range_part
        cur.execute(f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE \'range_part%\';")
        number_of_partitions = cur.fetchone()[0]

        # Dựa vào giá trị rating của bản ghi mới, tìm được số thứ tự mảnh phù hợp
        # Từ số thứ tự, tìm được tên bảng rồi chèn bản ghi vào như thông thường
        d = 5 / number_of_partitions
        i = int(rating / d)
        if rating % d == 0 and i != 0:
            i = i - 1
        tb_name = f'range_part{i}'
        cur.execute(f"INSERT INTO {tb_name} (userid, movieid, rating) values ({userid}, {movieid}, {rating})")

        cur.close()
        openconnection.commit()
    except Exception as ex: 
        openconnection.rollback()
        print(f'Chèn dữ liệu vào phân mảng ngang theo khoảng thất bại: {str(ex)}')

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Based on round-robin distribution, create new partitions from main table (ratings).
    """
    try:
        cur = openconnection.cursor()
        prefix = 'rrobin_part'

        # Tạo và điền dữ liệu vào các bảng phân mảnh
        for i in range(numberofpartitions):
            tb_name = f'{prefix}{i}'
            cur.execute(f"""
                CREATE TABLE {tb_name} AS
                SELECT userid, movieid, rating
                FROM (
                    SELECT userid, movieid, rating,
                           ROW_NUMBER() OVER () - 1 AS rnum
                    FROM {ratingstablename}
                ) AS temp
                WHERE MOD(rnum, {numberofpartitions}) = {i}
            """)
        cur.close()
        openconnection.commit()
        print(f"Phân mảnh round-robin hoàn thành với {numberofpartitions} bảng.")
    except Exception as ex:
        openconnection.rollback()
        print(f'Phân mảng ngang theo round-robin thất bại: {str(ex)}')

def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    """
    Insert a new record into the appropriate round-robin partition table.
    """
    try:
        cur = openconnection.cursor()
        prefix = 'rrobin_part'

        # Đếm số lượng bảng phân mảnh
        cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE %s", (f'{prefix}%',))
        number_of_partitions = cur.fetchone()[0]
        if number_of_partitions == 0:
            raise Exception("Không tìm thấy bảng phân mảnh round-robin.")

        # Xác định bảng đích dựa trên tổng số bản ghi hiện tại
        cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(movieid) + "," + str(rating) + ");")
        cur.execute("SELECT COUNT (*) FROM " + ratingstablename + ";");
        total_rows = (cur.fetchall())[0][0];
        partition_index = (total_rows - 1) % number_of_partitions
        tb_name = f'{prefix}{partition_index}'

        # Chèn dữ liệu vào bảng phân mảnh
        cur.execute(f"""
            INSERT INTO {tb_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """, (userid, movieid, rating))

        cur.close()
        openconnection.commit()
        print(f"Chèn dữ liệu vào {tb_name} thành công.")
    except Exception as ex:
        openconnection.rollback()
        print(f'Chèn dữ liệu vào phân mảng ngang theo round-robin thất bại: {str(ex)}')

def drop_and_init_db(dbname, connection):
    """
    Check if the database exists, drop it if it does, and create a new one.
    """
    try:
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = connection.cursor()

        # Close all connection to this db and drop
        cur.execute(
            'SELECT pg_terminate_backend(pg_stat_activity.pid) '
            'FROM pg_stat_activity '
            'WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid()',
            (dbname,)
        )
        cur.execute(f'DROP DATABASE IF EXISTS {psycopg2.extensions.quote_ident(dbname, cur)}')

        # Create new database
        cur.execute(f'CREATE DATABASE {psycopg2.extensions.quote_ident(dbname, cur)}')
        cur.close()
    except Exception as ex: 
        print(f'Kiểm tra/khởi tạo db thất bại: {str(ex)}')

def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE %s;", (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count

# Test cục bộ
# if __name__ == '__main__':
#     db_name = 'dds_assgn1'
#     user = 'postgres'
#     password = 'admin'
#     host = 'localhost'
#     port = 5432
#     rating_tb_name = 'ratings'
#     range_tb_prefix = 'range_part'
#     rrobin_tb_prefix = 'rrobin_part'

#     print('Xóa dữ liệu và tạo mới database ...')
#     default_connection = psycopg2.connect(database='postgres', user=user, password=password, host=host, port=port)
#     start_time = time.perf_counter()
#     drop_and_init_db(dbname=db_name, connection=default_connection)
#     default_connection.close()
#     print(f"Thời gian thực thi: {time.perf_counter() - start_time:.3f}s\n")

#     try:
#         with psycopg2.connect(database=db_name, user=user, password=password, host=host, port=port) as connection:

#             print('Khởi tạo dữ liệu bảng ratings ...')
#             start_time = time.perf_counter()
#             loadratings(rating_tb_name, 'ratings.dat', connection)
#             print(f"Thời gian thực thi: {time.perf_counter() - start_time:.3f}s\n")

#             print('Phân mảnh ngang theo khoảng ...')
#             start_time = time.perf_counter()
#             roundrobinpartition(rating_tb_name, 5, connection)
#             print(f"Thời gian thực thi: {time.perf_counter() - start_time:.3f}s\n")

#             print('Chèn dữ liệu vào phân mảnh ngang theo khoảng ...')
#             start_time = time.perf_counter()
#             roundrobininsert('', 1, 122, 5, connection)
#             print(f"Thời gian thực thi: {time.perf_counter() - start_time:.3f}s\n")

#     except Exception as ex:
#         print(f'Something went wrong: {str(ex)}')