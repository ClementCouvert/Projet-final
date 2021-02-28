#Couvert Clément

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master('local').enableHiveSupport().appName('LibraryManagement').getOrCreate()

#0 Creation des donnees
#author
list1 = [('07890', 'Jean Paul Sartre'), ('05678', 'Pierre de Ronsard')]
rdd1 = spark.sparkContext.parallelize(list1)
Author = rdd1.toDF(['aid', 'name'])
Author.createOrReplaceTempView('AuthorSQL')
Author.show()
#book
list2 = [('0001', "L'existentialisme est un humanisme", 'Philosophie'), \
      ('0002', 'Huis clos. Suivi de Les Mouches', 'Philosophie'), \
     ('0003', 'Mignonne allons voir si la rose', 'Poeme'), \
     ('0004', 'Les Amours', 'Poème')]
rdd2 = spark.sparkContext.parallelize(list2)
book = rdd2.toDF(['bid', 'title', 'category'])
book.createOrReplaceTempView('bookSQL')
book.show()
#student
list3 = [('S15', 'toto', 'Math'), \
      ('S16', 'popo', 'Eco'), \
     ('S17', 'fofo', 'Mécanique')]
rdd3 = spark.sparkContext.parallelize(list3)
Student = rdd3.toDF(['sid', 'sname', 'dept'])
Student.createOrReplaceTempView('StudentSQL')
Student.show()
#write
list4 = [('07890', '0001'), \
      ('07890', '0002'), \
     ('05678', '0003'), \
     ('05678', '0003')]
rdd4 = spark.sparkContext.parallelize(list4)
write = rdd4.toDF(['aid', 'bid'])
write.createOrReplaceTempView('writeSQL')
write.show()
#barrow
list5 = [('S15', '0003', '02-01-2020', '01-02-2020'), \
      ('S15', '0002', '13-06-2020', 'null'), \
     ('S15', '0001', '13-06-2020', '13-10-2020'), \
     ('S16', '0002', '24-01-2020', '24-01-2020'), \
     ('S17', '0001', '12-04-2020', '01-07-2020')]
rdd5 = spark.sparkContext.parallelize(list5)
borrow = rdd5.toDF(['sid', 'bid', 'checkout_time', 'return_time'])
borrow.createOrReplaceTempView('borrowSQL')
borrow.show()


#SQL
#1
spark.sql("""select title
            from bookSQL
            join borrowSQL
            on bookSQL.bid = borrowSQL.bid
            where borrowSQL.sid = 'S15' """).show()
#2
spark.sql("""select title 
            from bookSQL
            left join borrowSQL
            on bookSQL.bid = borrowSQL.bid
            where borrowSQL.bid is null""").show()
#3
spark.sql("""select sname
            from StudentSQL
            join borrowSQL
            on StudentSQL.sid = borrowSQL.sid
            where borrowSQL.bid = '0002' """).show()
#4
spark.sql("""select title
            from bookSQL
            join borrowSQL
            on bookSQL.bid = borrowSQL.bid
            join StudentSQL
            on borrowSQL.sid = StudentSQL.sid
            where StudentSQL.dept = 'Mécanique' """).show()
#5
spark.sql("""select sname
            from StudentSQL
            left join borrowSQL
            on StudentSQL.sid = borrowSQL.sid
            where borrowSQL.sid is null""").show()
#6
spark.sql("""select first(name) as auteur, count(distinct bid) as nombre
            from AuthorSQL
            join writeSQL
            on AuthorSQL.aid = writeSQL.aid
            group by name""").show()
#7
spark.sql("""select sname
            from StudentSQL
            join borrowSQL
            on StudentSQL.sid = borrowSQL.sid
            where borrowsql.return_time = 'null' """).show()
#8

#9 meme question que 2

#DSL
#1
book\
    .join(borrow,['bid'])\
    .join(student,['sid'])\
    .filter(F.col('sid')=='S15')\
    .select('sid','title')\
    .show()
#2
book.join(borrow, book.bid==borrow.bid, how='left')\
    .select('title')\
    .filter(F.col('sid').isNull())\
    .show()
#3
Student.join(borrow, 'sid')\
    .select('sname')\
    .filter(F.col('bid')=='0002')\
    .show()
#4
book.join(borrow, 'bid')\
    .join(Student, 'sid')\
    .select('title')\
    .filter(F.col('dept')=='Mécanique')\
    .show()
#5
Student.join(borrow, Student.sid==borrow.sid, how='left')\
    .select('sname')\
    .filter(F.col('sname').isNull())\
    .show()
#6
Author.join(write, "aid") \
    .distinct() \
    .groupBy("name") \
    .agg(F.count("bid").alias("nombre")) \
    .sort(F.col("nombre").desc()) \
    .select(F.first("name").alias("auteur"),F.first("nombre").alias("nombre")) \
    .show()
#7
Student.join(borrow, 'sid')\
    .select('sname')\
    .filter(F.col('return_time')=='null')\
    .show()
#8

#9 meme question que 2