select current_database() as database,
       current_schema() as schema,
       current_user as user,
       current_setting('ssl') as ssl_status
