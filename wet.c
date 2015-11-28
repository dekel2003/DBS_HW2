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

	return 0;
}