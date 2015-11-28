#include "wet.h"
#include <libpq-fe.h>

PGconn *conn;

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
			"VALUES ( (SELECT MAX(id) FROM users) +1 , '%s' )", name );	
			
	
	res = PQexec(conn,cmd);
	
	if(!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Error executing query: %s\n",
		PQresultErrorMessage(res));
		PQclear(res);
	}
	
	res = PQexec(conn,"(SELECT MAX(id) FROM users)");
	
	char* id = PQgetvalue(res, 0, 0);
	printf(ADD_USER, id);
}
/*
Find the first row where there does not exist a row with Id + 10

SELECT TOP 1 t1.Id+1 
FROM table t1
WHERE NOT EXISTS(SELECT * FROM table t2 WHERE t2.Id = t1.Id + 1)
ORDER BY t1.Id

<<<<<<< HEAD
*/
=======

void* addUserMin        (const char*    name){}
void* removeUser        (const char*    id){}
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
>>>>>>> 9d5ca3b200e86ef1656093522b7641959a9110a2
