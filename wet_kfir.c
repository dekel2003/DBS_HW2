#include <stdio.h>
#include <string.h>
#include <libpq-fe.h>
#include "wet.h"

static PGconn     *conn; 

int main() {
	/* Make a connection to the DB. If parameters omitt
ed, default values are 
used */
	char connect_param[200];
	sprintf(connect_param,	"host=csl2.cs.technion.ac.il dbname=%s user=%s password=%s",
	USERNAME, USERNAME, PASSWORD);
	conn = PQconnectdb(connect_param);
	/* check to see that the backend connection was suc
cessfully made */
	if (!conn || PQstatus(conn) == CONNECTION_BAD) {
		fprintf(stderr, "Connection to server failed: %s\n" ,PQerrorMessage(conn));
		PQfinish(conn);
		return 1;
	}
	parseInput();
	/* Close the connection to the database and cleanup */
	PQfinish(conn);
	return 0;
	
}

#define doQuery(res,query) do {\
		res = PQexec(conn,query);\
		if(!res || PQresultStatus(res) != PGRES_TUPLES_OK){\
			fprintf(stderr, "Error executing query: %s\n", PQresultErrorMessage(res));\
			PQclear(res);\
			return;\
		}} while(0)

#define doNonQuery(res,cmd) do {\
		res = PQexec(conn,cmd);\
		if(!res || PQresultStatus(res) != PGRES_COMMAND_OK){\
			fprintf(stderr, "Error executing cmd: %s\n", PQresultErrorMessage(res));\
			PQclear(res);\
			return;\
		}} while(0)


void* addUser  (const char*    name){
	PGresult *res_id , *res_add ;
	
	// query for user id
	doQuery(res_id,"SELECT COALESCE(MAX(id)+1, 0) FROM users");
	char id [256];
	sprintf(id ,PQgetvalue(res_id, 0, 0)); //number of users is next id.
	PQclear(res_id);
	
	// add the user	
	char cmd [256];
	sprintf (cmd,"INSERT INTO users VALUES(%s,'%s');",id,name);
	doNonQuery(res_add,cmd);	
	printf(ADD_USER,id);
	
	// clear data
	PQclear(res_add);		
}
void* addUserMin        (const char*    name){
	PGresult *res_id , *res_add , *res_print;
	int i;
	
	// query user min id
	doQuery(res_id,"SELECT COALESCE(MIN(id)+1, 0) FROM users u WHERE NOT EXISTS ( SELECT id FROM users where id = u.id+1 )");
	char id [256];
	sprintf(id,PQgetvalue(res_id, 0, 0));
	PQclear(res_id);
	
	// add the user	
	char cmd [256];
	sprintf (cmd,"INSERT INTO users VALUES(%s,'%s');",id,name);
	doNonQuery(res_add,cmd);	
	PQclear(res_add);
	
	
	//query all user with the input name
	char query [256];
	sprintf(query,"SELECT * FROM users WHERE name='%s' ORDER BY id",name);	
	doQuery(res_print,query);
		
	//print all users with input name
	printf(USER_HEADER);
	for(i=0;i<PQntuples(res_print);i++){
		printf(USER_RESULT,PQgetvalue(res_print, i, 0),PQgetvalue(res_print, i, 1));
	}	
	PQclear(res_print);
}
void* removeUser  	(const char*    id){
	PGresult *res_cmd, *res_query;
	
	//check if user exist
	char query [256];
	sprintf(query,"SELECT id FROM users WHERE id = %s",id);
	doQuery(res_query,query);
	if (PQntuples(res_query) == 0){ //if user is illegal
		PQclear(res_query);
		printf(ILL_PARAMS);
		return;
	}	
	PQclear(res_query);
	
	//delete user;
	char cmd [256];
	sprintf(cmd , "DELETE FROM users WHERE id = '%s'; DELETE FROM photos WHERE user_id = '%s' ; DELETE FROM  tags WHERE user_id = '%s'",id,id,id);
	doNonQuery(res_cmd,cmd);
	PQclear(res_cmd);
	
}
void* addPhoto          (const char*    user_id, const char*    photo_id){
	PGresult *res_id, *res_exist, *res_add;
	
	// check if user_id is ok
	char query [256];
	sprintf(query,"SELECT id FROM users WHERE id = %s",user_id);
	doQuery(res_id,query);
	if (PQntuples(res_id) == 0){
		PQclear(res_id);
		printf(ILL_PARAMS);
		return;
	}		
	PQclear(res_id);
	
	//check that this is a new photo
	sprintf(query,"SELECT * FROM photos WHERE user_id = %s AND id=%s",user_id,photo_id);
	doQuery(res_exist,query);
	if (PQntuples(res_exist) != 0){
		PQclear(res_exist);
		printf(EXISTING_RECORD);
		return;
	}	
	PQclear(res_exist);
	
	// insert new photo
	char cmd [256];
	sprintf(cmd,"INSERT INTO photos(id,user_id) VALUES(%s,'%s');",photo_id,user_id);
	doNonQuery(res_add,cmd);
	PQclear(res_add);
	
	
}
void* tagPhoto          (const char*    user_id, const char*    photo_id, const char*    info){
	PGresult *res_id, *res_exist, *res_add;
	
	// check if user_id is ok
	char query [256];
	sprintf(query,"SELECT id FROM users WHERE id = %s",user_id);
	doQuery(res_id,query);
	if (PQntuples(res_id) == 0){
		PQclear(res_id);
		printf(ILL_PARAMS);
		return;
	}		
	PQclear(res_id);
	
	//check that this is a new tag
	sprintf(query,"SELECT * FROM tags WHERE user_id = %s AND photo_id=%s AND info = '%s'",user_id,photo_id,info);
	doQuery(res_exist,query);
	if (PQntuples(res_exist) != 0){
		PQclear(res_exist);
		printf(EXISTING_RECORD);
		return;
	}	
	PQclear(res_exist);
	
	//insert new tag
	char cmd [256];
	sprintf(cmd,"INSERT INTO tags(photo_id,user_id,info) VALUES(%s,%s,'%s');",photo_id,user_id,info);
	doNonQuery(res_add,cmd);
	PQclear(res_add);
}
void* photosTags        (){
	PGresult *res_query;
	int i;
	
	char query [256];
	sprintf(query,"SELECT photos.user_id, photos.id, COUNT(info) as num FROM photos LEFT OUTER JOIN tags  "
									"ON(tags.photo_id = photos.id AND tags.user_id = photos.user_id)  GROUP BY photos.id,photos.user_id "
									"ORDER BY num DESC , photos.user_id ,photos.id");
	doQuery(res_query,query);
	if (PQntuples(res_query) == 0){
		PQclear(res_query);
		printf(EMPTY);
		return;
	}
	printf(PHOTOS_HEADER);
	for(i=0;i<PQntuples(res_query);i++){
		printf(PHOTOS_RESULT,PQgetvalue(res_query, i, 0),PQgetvalue(res_query, i, 1),PQgetvalue(res_query, i, 2));
	}	
	PQclear(res_query);		
}
void* search            (const char*    word){
	PGresult *res_query;
	int i;
	
	char query [256];
	sprintf(query,"SELECT user_id, photo_id, COUNT(info) as num FROM tags T "
									"WHERE EXISTS (SELECT user_id,photo_id FROM tags WHERE info LIKE '%%%s%%' AND T.user_id=user_id AND T.photo_id=photo_id) " 
									"GROUP BY photo_id,user_id "
									"ORDER BY num DESC , user_id ,photo_id DESC",word);
	
	doQuery(res_query,query);
	if (PQntuples(res_query) == 0){
		PQclear(res_query);
		printf(EMPTY);
		return;
	}
	printf(PHOTOS_HEADER);
	for(i=0;i<PQntuples(res_query);i++){
		printf(PHOTOS_RESULT,PQgetvalue(res_query, i, 0),PQgetvalue(res_query, i, 1),PQgetvalue(res_query, i, 2));
	}	
	PQclear(res_query);		
	
}
void* commonTags        (const char*    k){
	PGresult *res_query;
	int i;
	
	char query [256];
	sprintf(query,"SELECT info ,COUNT(*) as num FROM tags GROUP BY info HAVING COUNT(*) >= %s ORDER BY num DESC,info ",k);
	
	doQuery(res_query,query);
	if (PQntuples(res_query) == 0){
		PQclear(res_query);
		printf(EMPTY);
		return;
	}
	printf(COMMON_HEADER);
	for(i=0;i<PQntuples(res_query);i++){
		printf(COMMON_LINE,PQgetvalue(res_query, i, 0),PQgetvalue(res_query, i, 1));
	}	
	PQclear(res_query);		
}
void* mostCommonTags    (const char*    k){
	PGresult *res_query ,*res_view ,*res_delete;
	int i;
	
	char cmd [1024];
	sprintf(cmd , "CREATE VIEW tmp AS SELECT info ,COUNT(*) as num FROM tags GROUP BY info");
	doNonQuery(res_view,cmd);
	PQclear(res_view);
	
	char query [1024];
	sprintf(query, 
	"SELECT T.info, T.num FROM tmp T "
		"WHERE (SELECT COUNT(*) FROM tmp T2 WHERE T2.num > T.num OR (T2.num = T.num AND T2.info < T.info)) < %s "
		"ORDER BY T.num DESC, T.info" , k);
	doQuery(res_query,query);
	
	char cmd2 [1024];
	sprintf(cmd2, "DROP VIEW tmp;");
	doNonQuery(res_delete,cmd2);
	PQclear(res_delete);
	
	if (PQntuples(res_query) == 0){
		PQclear(res_query);
		printf(EMPTY);
		return;
	}
	
	printf(COMMON_HEADER);
	for(i=0;i<PQntuples(res_query);i++){
		printf(COMMON_LINE,PQgetvalue(res_query, i, 0),PQgetvalue(res_query, i, 1));
	}	
	PQclear(res_query);	
	
	
}
void* similarPhotos     (const char*    k, const char*    j){
	PGresult *res_query;
	int i;
	char query [1024];
	sprintf(query , "SELECT T.id , U.name , T.photo_id FROM ("
			"SELECT T1.user_id as id, T1.photo_id as photo_id FROM tags T1, tags T2 "
				"WHERE T1.info = T2.info AND (T1.user_id <> T2.user_id OR T1.photo_id <> T2.photo_id) "
				"GROUP BY T1.user_id, T1.photo_id, T2.user_id, T2.photo_id "
				"HAVING COUNT(*) >= %s ) T ,users U "
			"WHERE T.id = U.id "
			"GROUP BY T.id , U.name , T.photo_id "
			"HAVING COUNT(*) >= %s"
			"ORDER BY T.id, T.photo_id", j,k);
		
	doQuery(res_query,query);
	if (PQntuples(res_query) == 0){
		PQclear(res_query);
		printf(EMPTY);
		return;
	}
	printf(SIMILAR_HEADER);
	for(i=0;i<PQntuples(res_query);i++){
		printf(SIMILAR_RESULT,PQgetvalue(res_query, i, 0),PQgetvalue(res_query, i, 1),PQgetvalue(res_query, i, 2));
	}	

	PQclear(res_query);	
}
void* autoPhotoOnTagOn  (){
	PGresult *res_func,*res_init;
	char* trigger_func = "CREATE OR REPLACE FUNCTION add_missing_nodes() "
	"RETURNS TRIGGER AS $$ "
		"BEGIN "
			"IF (NEW.user_id  IN (SELECT id FROM users ) AND (NEW.photo_id , NEW.user_id) NOT IN (SELECT * FROM photos)) THEN "
				"INSERT INTO photos(user_id,id) VALUES(NEW.user_id,NEW.photo_id); "
			"END IF; "
		"RETURN NEW; "
		"END; "
	"$$ LANGUAGE plpgsql; ";
	
	char* triget_init = "CREATE TRIGGER photoOnTag "
		"BEFORE INSERT ON tags "
		"FOR EACH ROW EXECUTE PROCEDURE add_missing_nodes(); ";
		
	doNonQuery(res_func,trigger_func);
	PQclear(res_func);
	
	doNonQuery(res_init,triget_init);
	PQclear(res_init);
}
	
void* autoPhotoOnTagOFF (){
	PGresult *res_trigger,*res_func;
	char* remove_trigger = "DROP TRIGGER IF EXISTS  photoOnTag ON tags";
	char* remove_function = "DROP FUNCTION IF EXISTS add_missing_nodes()";
	
	doNonQuery(res_trigger,remove_trigger);
	PQclear(res_trigger);
	
	doNonQuery(res_func,remove_function);
	PQclear(res_func);
		
}