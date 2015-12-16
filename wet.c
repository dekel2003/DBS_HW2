#include "wet.h"
#include <libpq-fe.h>

PGconn *conn;


#define SQL_QRY(query) do {\
		PGresult *res = PQexec(conn,query);\
		if(!res || PQresultStatus(res) != PGRES_TUPLES_OK){\
			fprintf(stderr, "SQL Error in query: %s\n", PQresultErrorMessage(res));\
			PQclear(res);\
			return;\
		}} while(0)

#define SQL_CMD(cmd) do {\
		PGresult *res = PQexec(conn,cmd);\
		if(!res || PQresultStatus(res) != PGRES_COMMAND_OK){\
			fprintf(stderr, "SQL Error in cmd: %s\n", PQresultErrorMessage(res));\
			PQclear(res);\
			return;\
		}} while(0)


int main(void)
{
	char connect_param[200];
	
	sprintf(connect_param,
				"host=csl2.cs.technion.ac.il dbname=%s user=%s password=%s",
				USERNAME, USERNAME, PASSWORD);
				
	conn = PQconnectdb(connect_param);
	
	/* check to see that the backend connection was successfully made */
	if (!conn || PQstatus(conn) == CONNECTION_BAD)
	{
		fprintf(stderr, "Connection to server failed: %s\n",
		PQerrorMessage(conn));
		PQfinish(conn);
		return 1;
	}

	parseInput();
	
	/* Close the connection to the database and cleanup */
	PQfinish(conn);
	
	return 0;
}

void* addUser(const char* name)
{	
	char cmd[2000] = {0};
	PGresult *res;
	
	sprintf(cmd,"INSERT INTO users(id, name) "
			"VALUES ( (SELECT COALESCE(MAX(id), -1) FROM users) + 1 , '%s' );", name );	
			
			SQL_CMD(cmd);
	/*
	res = PQexec(conn,cmd);
	
	if(!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Error executing query: %s\n",
		PQresultErrorMessage(res));
		PQclear(res);
	}
	*/
	
	res = PQexec(conn,"(SELECT MAX(id) FROM users)");
	
	char* id = PQgetvalue(res, 0, 0);
	printf(ADD_USER, id);
	
	PQclear(res);
}

/*
Find the first row where there does not exist a row with Id + 10

SELECT TOP 1 t1.Id+1 
FROM table t1
WHERE NOT EXISTS(SELECT * FROM table t2 WHERE t2.Id = t1.Id + 1)
ORDER BY t1.Id
*/

void* addUserMin        (const char*    name)
{
	char cmd[2000] = {0};
	PGresult *res;
	
	sprintf(cmd,"INSERT INTO users(id, name) "
				"VALUES ( (select COALESCE(MIN(ID +1)) From users as t1 "
				"where not exists (select * from users as t2 where t1.id +1 = t2.id) ), '%s' )", name );
		
	res = PQexec(conn,cmd);
	
	if(!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Error executing query: %s\n",
		PQresultErrorMessage(res));
		PQclear(res);
	}

	res = PQexec(conn,"(SELECT id,name FROM users ORDER BY id)");
	
	printf(USER_HEADER);
	
	{
		int size = PQntuples(res);
		int i = 0;
		for( ; i < size; ++i)
		{
			char* id = PQgetvalue(res, i,0 );
			char* name = PQgetvalue(res, i,1 );
			printf(USER_RESULT,id, name);
		}
	}
	PQclear(res);
}
void* removeUser        (const char*    id)
{

}


void* addPhoto          (const char*    user_id,
                         const char*    photo_id){}
void* tagPhoto          (const char*    user_id,
                         const char*    photo_id,
                         const char*    info){}
void* photosTags        (){}
void* search            (const char*    word){}
void* commonTags        (const char*    k){}
void* mostCommonTags    (const char*    k){}
void* similarPhotos     (const char*    k,
                         const char*    j){}
void* autoPhotoOnTagOn  (){}
void* autoPhotoOnTagOFF (){}
