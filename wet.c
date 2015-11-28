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

	if(!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Error executing query: %s\n",
		PQresultErrorMessage(res));
		PQclear(res);
	}
	
	res = PQexec(conn,"(SELECT MAX(id) FROM users)");
	
	char* id = PQgetvalue(res, 0, 0);
	printf(ADD_USER, id);
}


