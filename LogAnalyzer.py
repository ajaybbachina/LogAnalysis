import psycopg2

dbName="dbname=news"
def main():
    while True:
        print("Select any of the below question:")
        print("1. What are the most popular three articles of all time?")
        print("2. Who are the most popular article authors of all time?")
        print("3. On which days did more than 1% of requests lead to errors?")
        
        selection=int(input())
        if(selection==1):
            query="select ac.title, count(*) as Views from articles ac "\
                "JOIN log lg on ac.slug = substring(lg.path, 10) GROUP BY ac.title ORDER BY Views DESC;"
            result=executeQuery(query)
            printResults(result," views")
        elif(selection==2):
            query="SELECT au.name, count(*) as views FROM articles ar " \
                "JOIN authors au ON ar.author = au.id "\
                "JOIN log lg "\
                "ON ar.slug = substring(lg.path, 10) "\
                "WHERE lg.status LIKE '200 OK' "\
                "GROUP BY au.name ORDER BY views DESC;"
            result=executeQuery(query)
            printResults(result," views")
        elif(selection==3):
            query="select * from "\
                "(select failedTable.selDate, round(failedTable.failed*100.0/fullTable.total,1) as Percentage from "\
                "(select to_char(lg1.time,'Mon DD,yyyy') as selDate,count(*) as failed  from log lg1 where status <>'200 OK' "\
                "group by selDate) as failedTable "\
                "Join (select to_char(lg1.time,'Mon DD,yyyy') as selDate,count(*) as total  from log lg1 group by selDate) as fullTable "\
                "on fullTable.selDate=failedTable.selDate) as Results "\
                "where Results.Percentage>1 "\
                "order by Results.Percentage desc;"
            result=executeQuery(query)
            printResults(result,'% errors')
        else:
            print("Incorrect Selection !!")
        
        print("Do you want to make another selection y-yes or any other key to exit.")
        if(input()!='y'):
            break

def printResults(result,resultType):
    for i in range(len(result)):
        print("\t" + "%s - %s" % (result[i][0], result[i][1]) + resultType)


def executeQuery(query):
    try:
        conn = psycopg2.connect(dbName)
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        conn.close()
        return results
       
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

main()