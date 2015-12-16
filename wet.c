#include "wet.h"
#include <libpq-fe.h>

PGconn *conn;


PGresult* EXE_SQL_QRY(const char* query){
	PGresult* res = PQexec(conn,query);
	if(!res || PQresultStatus(res) != PGRES_TUPLES_OK){
		fprintf(stderr, "SQL Error in query: %s\n", PQresultErrorMessage(res));
		PQclear(res);
		return NULL;
	}
	return res;
} 
		
void EXE_SQL_CMD(const char* cmd){
	PGresult* res = PQexec(conn,cmd);
	if(!res || PQresultStatus(res) != PGRES_COMMAND_OK){
		fprintf(stderr, "SQL Error in cmd: %s\n", PQresultErrorMessage(res));
	}
	PQclear(res);
} 


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
{	PGresult *res;
	char cmd[2000] = {0};
	char query[2000] = {0};
	
	sprintf(cmd,"INSERT INTO users(id, name) "
			"VALUES ( (SELECT COALESCE(MAX(id), -1) FROM users) + 1 , '%s' );", name );	
			
	EXE_SQL_CMD(cmd);

	sprintf(query,"(SELECT MAX(id) FROM users)");
	

	res = EXE_SQL_QRY(query);
	if (!res)
		return NULL;
	
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

void* addUserMin(const char*    name)
{
	char cmd[2000] = {0};
	char query[2000] = {0};
	PGresult *res;
	
	sprintf(cmd,"INSERT INTO users(id, name) "
				"VALUES ( (select COALESCE(MIN(ID +1)) From users as t1 "
				"where not exists (select * from users as t2 where t1.id +1 = t2.id) ), '%s' )", name );
		
	EXE_SQL_CMD(cmd);

	sprintf(query,"(SELECT id,name FROM users ORDER BY id)");
	res = EXE_SQL_QRY(query);
	if (!res)
		return NULL;
	
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

int userExist(const char* id){
	char qry[2000] = {0};
	
	sprintf(qry,"SELECT id FROM users WHERE id = %s",id);
	
	PGresult *res = EXE_SQL_QRY(qry);

	if ( 0 == PQntuples(res)){
		PQclear(res);
		printf(ILL_PARAMS);
		return 0;
	}	
	PQclear(res);
	return 1;
}

void* removeUser(const char* id)
{
	if (0 == userExist(id)) return;
	
	char cmd[2000] = {0};
	sprintf(cmd,"delete from users where users.id=%s; delete from photos "
			    "where user_id=%s; delete from tags where user_id=%s;",id, id, id);
				
	EXE_SQL_CMD(cmd);
}


void* addPhoto          (const char*    user_id,
                         const char*    photo_id){				 
	if (0 == userExist(user_id)) return;	

	/*Check if photo+user in photos. */
	char qry[2000] = {0};
	sprintf(qry,"select user_id from photos as t where t.user_id = '%s' AND t.id = '%s'",user_id, photo_id);
	PGresult *res = EXE_SQL_QRY(qry);
	if ( 0 < PQntuples(res)){
		PQclear(res);
		printf(EXISTING_RECORD);
		return 0;
	}	
	PQclear(res);
	
	char cmd[2000] = {0};
	sprintf(cmd, "INSERT INTO photos(id, user_id) VALUES(%s,%s)", photo_id ,user_id );
	EXE_SQL_CMD(cmd);					 
}


void* tagPhoto          (const char*    user_id,
                         const char*    photo_id,
                         const char*    info){
	if (0 == userExist(user_id)) return;
	
	/*Check if photo+user+info in photos. */
	char qry[2000] = {0};
	sprintf(qry,"select user_id from tags as t "
				"where t.user_id = '%s' AND t.photo_id = '%s' AND t.info = %s",user_id, photo_id,info);
	PGresult *res = EXE_SQL_QRY(qry);
	if ( 0 < PQntuples(res)){
		PQclear(res);
		printf(EXISTING_RECORD);
		return 0;
	}	
	PQclear(res);					 
	
	char cmd[2000] = {0};
	sprintf(cmd, "INSERT INTO tags(photo_id, user_id,info) VALUES(%s,%s,%s)", photo_id ,user_id,info );
	EXE_SQL_CMD(cmd);	
}

void* photosTags        (){
	char qry[2000] = {0};
	sprintf(qry,"SELECT photos.user_id, photos.id, COUNT(info) as n "
				"from photos "
				"left outer join tags "
				"ON(photos.id = tags.photo_id AND photos.user_id = tags.user_id) "
				"GROUP BY photos.user_id, photos.id "
				"ORDER BY n DESC , photos.user_id ,photos.id");
				
	PGresult *res = EXE_SQL_QRY(qry);
	
	int size = PQntuples(res);
	if ( 0 == size){
		printf(EMPTY);
	}else{
		printf(PHOTOS_HEADER);
		{
			int i = 0;
			for( ; i < size; ++i)
			{
				char* id = PQgetvalue(res, i,0 );
				char* pid = PQgetvalue(res, i,1 );
				char* count = PQgetvalue(res, i,2 );
				printf(PHOTOS_RESULT,id, pid, count);
			}
		}
	}
	PQclear(res);
}

void* search            (const char*    word){
	char qry[2000] = {0};
	sprintf(qry,"select photo_id, user_id, count(info) as n "
				"from tags t "
				"where t.info LIKE '%r%' "
				"group by photo_id, user_id "
				"GROUP BY photos.user_id, photos.id "
				"ORDER BY n DESC , user_id ,photo_id DESC");
	PGresult *res = EXE_SQL_QRY(qry);
	
	int size = PQntuples(res);
	if ( 0 == size){
		printf(EMPTY);
	}else{
		printf(PHOTOS_HEADER);
		{
			int i = 0;
			for( ; i < size; ++i)
			{
				char* pid = PQgetvalue(res, i,0 );
				char* id = PQgetvalue(res, i,1 );
				char* count = PQgetvalue(res, i,2 );
				printf(PHOTOS_RESULT,count,id, pid);
			}
		}
	}
	PQclear(res);			
}

void* commonTags        (const char*    k){}
void* mostCommonTags    (const char*    k){}
void* similarPhotos     (const char*    k,
                         const char*    j){}
void* autoPhotoOnTagOn  (){}
void* autoPhotoOnTagOFF (){}
