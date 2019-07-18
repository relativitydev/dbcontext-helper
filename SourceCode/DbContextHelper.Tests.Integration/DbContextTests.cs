using DbContextHelper;
using NUnit.Framework;
using Relativity.API.Context;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;

namespace DBContextHelper.Tests.Integration
{
	[TestFixture]
	public class DbContextTests
	{
		#region variables

		public DbContext Sut { get; set; }

		private string _sql;

		//Insert configurations
		private const string SqlServerAddress = "localhost";
		private const string SqlUserName = "sql_user_name";
		private const string SqlPassword = "sql_password";

		#endregion

		#region Setup

		[SetUp]
		public void Setup()
		{
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

		[Test]
		public void ExecuteNonQuerySQLStatement_Valid_Sql_String()
		{
			//Arrange
			const string newValue = "ABC";
			_sql = $"UPDATE [EDDS].[EDDSDBO].[Artifact] SET [Keywords] = '{newValue}' WHERE [ArtifactID] = -1";
			const string querySql = "SELECT TOP 1 [Keywords] FROM [EDDS].[EDDSDBO].[Artifact] WHERE [ArtifactID] = -1";
			string originalValue = Sut.ExecuteSqlStatementAsScalar<string>(querySql, null, 30);
			string cleanUpSql = $"UPDATE [EDDS].[EDDSDBO].[Artifact] SET [Keywords] = '{originalValue}' WHERE [ArtifactID] = -1";

			//Act
			Sut.ExecuteNonQuerySQLStatement(_sql, null, 30);

			//Assert
			string queriedValue = Sut.ExecuteSqlStatementAsScalar<string>(querySql, null, 30);
			Assert.AreEqual(queriedValue, newValue);

			//Cleanup
			_sql = "";
			Thread.Sleep(TimeSpan.FromSeconds(10));
			Sut.ExecuteNonQuerySQLStatement(cleanUpSql, null, 30);
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
		public void ExecuteSQLStatementAsEnumerable_With_Timeout_Valid_Sql_String()
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
		public void ExecuteSQLStatementAsEnumerable_With_SqlParameters_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT [ArtifactID], [Name] FROM [EDDS].[EDDSDBO].[Case] WHERE NAME = @caseName";
			const string caseName = "New Case Template";
			IEnumerable<SqlParameter> sqlParameters = new List<SqlParameter>{
				new SqlParameter
				{
					ParameterName = "@caseName",
					SqlDbType = SqlDbType.NVarChar,
					Direction = ParameterDirection.Input,
					Value = caseName
				}};

			//Act
			IEnumerable<string> value = Sut.ExecuteSqlStatementAsEnumerable<string>(_sql, ConvertToString, sqlParameters);

			//Assert
			Assert.IsNotNull(value);
			Assert.Greater(Convert.ToInt32(value.FirstOrDefault()), 1);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlStatementAsObject_Valid_Sql_String()
		{
			//Arrange
			_sql = "SELECT TOP 1 [ArtifactID], [Name] FROM [EDDS].[EDDSDBO].[Case] WHERE NAME = @caseName";
			const string caseName = "New Case Template";
			IEnumerable<SqlParameter> sqlParameters = new List<SqlParameter>{
				new SqlParameter
				{
					ParameterName = "@caseName",
					SqlDbType = SqlDbType.NVarChar,
					Direction = ParameterDirection.Input,
					Value = caseName
				}};

			//Act
			string value = Sut.ExecuteSqlStatementAsObject<string>(_sql, ConvertToString, sqlParameters, 30);

			//Assert
			Assert.IsNotNull(value);
			Assert.Greater(Convert.ToInt32(value), 1);

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
