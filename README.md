# dbcontext-helper
Open Source Community: We have a created a helper to accommodate the removal of the implementation of the IDBContext contract mentioned in this community post https://community.relativity.com/s/feed/0D55000005WgPOUCA3 The DBContext class will no longer be exposed for construction or reference starting June 2018 release of Relativity One. To minimize impact to any of the our developers that have standalone applications(Console applications, Integration tests, etc) and need to create an instance of the DBcontext object, this helper will help you do that. For all Relativity extensibility points(Event Handlers, Custom Pages, Agents, etc), please convert the instantiation of a DBContext object with a call to the appropriate Helper method (see code example below). Only reference and develop against the IDBContext interface.

Usage example:

```
public IDBContext GetDBContext()
{
	DbContextHelper.DbContext dbContext = new DbContextHelper.DbContext("SQL_SERVER_ADDRESS", "SQL_DATABASE_NAME", "SQL_USER_NAME", "SQL_PASSWORD");
	return dbContext;
}
```

You can also find the implementation of DbContextHelper.DbContext in the [relativity-test-helpers](https://github.com/relativitydev/relativity-test-helpers) Github project.
