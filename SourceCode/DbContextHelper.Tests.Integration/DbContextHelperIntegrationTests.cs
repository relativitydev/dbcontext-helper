using DbContextHelper;
using kCura.Relativity.Client;
using NUnit.Framework;
using Relativity.API.Context;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace DBContextHelper.Tests.Integration
{
	[TestFixture]
	public class DbContextHelperIntegrationTests
	{
		#region variables

		public DbContext Sut { get; set; }

		private string _sql;
		private IRSAPIClient _rsapiClient;
		private const string WorkspaceName = "DbContext Helper";
		private const string WorkspaceNameChange = "DbContext Utility";
		private const string TestWorkspaceTemplateName = "New Case Template";
		private static readonly Uri ServicesUri = new Uri(BaseRelativityUrl + ".Services");

		//Insert configurations
		private const string RelativityAdminUserName = "relativity_admin_user_name";
		private const string RelativityAdminPassword = "relativity_admin_password";
		private const string BaseRelativityUrl = "http://localhost/Relativity";
		private const string SqlServerAddress = "localhost";
		private const string SqlUserName = "sql_user_name";
		private const string SqlPassword = "sql_password";

		#endregion

		#region Setup

		[SetUp]
		public void Setup()
		{
			//create client
			_rsapiClient = RsapiHelper.CreateRsapiClient(ServicesUri, RelativityAdminUserName, RelativityAdminPassword);
			_rsapiClient.APIOptions.WorkspaceID = -1;
			Sut = new DbContext(SqlServerAddress, "EDDS", SqlUserName, SqlPassword);
		}

		#endregion

		#region Tests

		[Test]
		public void ExecuteSqlStatementAsDataTable_Valid_Sql_String()
		{
			//Arrange
			_sql = @"SELECT * FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			DataTable dt = Sut.ExecuteSqlStatementAsDataTable(_sql, 30, null);

			//Assert
			Assert.IsNotNull(dt);

			//Cleanup
			_sql = "";

		}

		[Test]
		public void ExecuteSqlStatementAsScalar_T_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT Count(*)  FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			int value = Sut.ExecuteSqlStatementAsScalar<int>(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);
			Assert.Greater(value, 1);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlStatementAsScalar_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT Count(*)  FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			object value = Sut.ExecuteSqlStatementAsScalar(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);
			Assert.Greater(Convert.ToInt32(value), 1);

			//Cleanup
			_sql = "";
		}

		/// <summary>
		/// This sql statement here is making an update to your database. PLEASE USE WITH CAUTION!!
		/// </summary>

		[Test]
		public void ExecuteNonQuerySQLStatement_Valid_Sql_String()
		{
			//Arrange
			int newWorkspaceArtifactId = RsapiHelper.CreateWorkspace(_rsapiClient, WorkspaceName, TestWorkspaceTemplateName);
			_sql = $"update [EDDS].[EDDSDBO].[Case] Set Name = '{WorkspaceNameChange}' where ArtifactID = '{newWorkspaceArtifactId}'";
			string sql2 = $"SELECT Name FROM [EDDS].[EDDSDBO].[Case] where ArtifactID = {newWorkspaceArtifactId} ";

			//Act
			Sut.ExecuteNonQuerySQLStatement(_sql, null, 30);
			object value = Sut.ExecuteSqlStatementAsScalar(sql2, null, 30);

			//Assert
			Assert.AreEqual(value.ToString(), WorkspaceNameChange);

			//Cleanup
			_sql = "";
			RsapiHelper.DeleteWorkspace(_rsapiClient, newWorkspaceArtifactId);
		}

		[Test]
		public void ExecuteSqlStatementAsDbDataReader_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT * FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			DbDataReader value = Sut.ExecuteSqlStatementAsDbDataReader(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementGetSecondDataTable_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT * FROM [EDDS].[EDDSDBO].[User]; SELECT * FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			DataTable value = Sut.ExecuteSQLStatementGetSecondDataTable(_sql, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementAsReader_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT * FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			SqlDataReader value = Sut.ExecuteSQLStatementAsReader(_sql, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteParameterizedSQLStatementAsReader_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT * FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			SqlDataReader value = Sut.ExecuteParameterizedSQLStatementAsReader(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementAsEnumerable_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT [ArtifactID], [Name] FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			IEnumerable<string> value = Sut.ExecuteSQLStatementAsEnumerable<string>(_sql, ConvertToString, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteProcedureAsReader_Valid_Sql_String()
		{
			//Arrange
			//Act
			SqlParameter sqlParameter1 = new SqlParameter
			{
				ParameterName = "@UserID",
				SqlDbType = SqlDbType.Int,
				Direction = ParameterDirection.Input,
				Value = 9
			};

			SqlParameter sqlParameter2 = new SqlParameter
			{
				ParameterName = "@ArtifactID",
				SqlDbType = SqlDbType.Int,
				Direction = ParameterDirection.Input,
				Value = 9
			};

			DbDataReader dbDataReader = Sut.ExecuteProcedureAsReader("GetUserPermissions", new List<SqlParameter> { sqlParameter1, sqlParameter2 });

			//Assert
			Assert.IsNotNull(dbDataReader);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlStatementAsDataSet_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT * FROM [EDDS].[EDDSDBO].[Case]";

			//Act
			DataSet dt = Sut.ExecuteSqlStatementAsDataSet(_sql, null, 30);

			//Assert
			Assert.IsNotNull(dt);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlBulkCopy_Valid()
		{
			//Arrange
			const string tempTableName = "DbContextHelperTempTable";

			//Drop Temp table if it already exists
			string dropTempTableIfItAlreadyExistsSql = $@"
				IF NOT OBJECT_ID('[EDDSDBO].[{tempTableName}]') IS NULL 
					BEGIN DROP TABLE [EDDSDBO].[{tempTableName}]
				END";
			Sut.ExecuteSqlStatementAsDataSet(dropTempTableIfItAlreadyExistsSql, null, 30);

			//Create Temp table
			string createTempTableSql = $"CREATE TABLE {tempTableName} (TestId int, TestCount int);";
			Sut.ExecuteSqlStatementAsDataSet(createTempTableSql, null, 30);

			//Create Sql Bulk Copy Parameters
			ISqlBulkCopyParameters bulkCopyParameters = new SqlBulkCopyParameters();
			SqlBulkCopyColumnMapping testId = new SqlBulkCopyColumnMapping("Id", "TestId");
			bulkCopyParameters.ColumnMappings.Add(testId);
			SqlBulkCopyColumnMapping count = new SqlBulkCopyColumnMapping("Count", "TestCount");
			bulkCopyParameters.ColumnMappings.Add(count);
			bulkCopyParameters.DestinationTableName = tempTableName;

			//Act
			using (IDataReader reader = GetSampleDataReader())
			{
				Sut.ExecuteSqlBulkCopy(reader, bulkCopyParameters);
			}

			//Cleanup
			//Drop Temp table if it already exists
			Sut.ExecuteSqlStatementAsDataSet(dropTempTableIfItAlreadyExistsSql, null, 30);
		}

		#endregion

		#region Private methods

		private static IDataReader GetSampleDataReader()
		{
			DataTable dt = new DataTable();
			dt.Columns.Add("Id", typeof(int));
			dt.Columns.Add("Count", typeof(int));
			dt.Rows.Add(1, 2);
			dt.Rows.Add(3, 4);
			IDataReader dataReader = dt.CreateDataReader();
			return dataReader;
		}

		private static string ConvertToString(SqlDataReader sqlDataReader)
		{
			string returnString = sqlDataReader.GetInt32(0).ToString();
			return returnString;
		}

		#endregion
	}
}
