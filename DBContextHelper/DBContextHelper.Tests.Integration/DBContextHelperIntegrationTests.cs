using kCura.Relativity.Client;
using kCura.Relativity.Client.DTOs;
using NUnit.Framework;
using Relativity.API;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Relativity.API.Context;
using SqlBulkCopyParameters = kCura.Data.RowDataGateway.SqlBulkCopyParameters;

namespace DBContextHelper.Tests.Integration
{
    [TestFixture]
	public class DbContextHelperIntegrationTests
	{
		#region variables

		private string _sql;
		private IServicesMgr _servicesManager;
		private IRSAPIClient _client;
		private int _workspaceId;
		private string _workspaceName = "DBContext Helper";
		private string _workspaceNameChange = "DBContext Utility";
		private string TEST_WORKSPACE_TEMPLATE_NAME = "New Case Template";
		public static int WorkspaceID = -1;
		public static Uri WebApiUri => new Uri(BaseRelativityURL + "webapi/");
		public static Uri ServicesUri => new Uri(BaseRelativityURL + ".Services");
		public static Uri RestUri => new Uri(BaseRelativityURL + ".REST/api");

		public DbContext sut { get; set; }

		//Insert configurations
		public static readonly string _userName = "";
		public static readonly string _password = "";
		public static readonly string BaseRelativityURL = "http://<server name >/Relativity";
		public static string SQL_SERVER_ADDRESS = "";
		public static string SQL_USER_NAME = "";
		public static string SQL_PASSWORD = "";

		#endregion

		#region Setup

		[SetUp]
		public void Setup()
		{
			//Setup for testing		
			//create client
			_client = GetRsapiClient();
			sut = new DbContext(SQL_SERVER_ADDRESS, string.Format("EDDS"), SQL_USER_NAME, SQL_PASSWORD);

		}

		#endregion

		#region Tests

		[Test]
		public void ExecuteSqlStatementAsDataTable_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DataTable dt = sut.ExecuteSqlStatementAsDataTable(_sql, 30, null);

			//Assert
			Assert.IsNotNull(dt);

			//Cleanup
			_sql = "";

		}

		[Test]
		public void ExecuteSqlStatementAsScalar_T_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT Count(*)  FROM [EDDS].[eddsdbo].[Case]");

			//Act
			int value = sut.ExecuteSqlStatementAsScalar<int>(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);
			Assert.Greater(value, 1);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlStatementAsScalar_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT Count(*)  FROM [EDDS].[eddsdbo].[Case]");

			//Act
			object value = sut.ExecuteSqlStatementAsScalar(_sql, null, 30);

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
		public void ExecuteNonQuerySQLStatement_Valid_sqlstring()
		{
			//Arrange
			_workspaceId = CreateWorkspace();
			_sql = String.Format(@"update [EDDS].[eddsdbo].[Case] Set Name = '{0}' where ArtifactID = '{1}'", _workspaceNameChange, _workspaceId);
			string _sql2 = String.Format(@"SELECT Name  FROM [EDDS].[eddsdbo].[Case] where ArtifactID = {0} ", _workspaceId);

			//Act
			sut.ExecuteNonQuerySQLStatement(_sql, null, 30);
			object value = sut.ExecuteSqlStatementAsScalar(_sql2, null, 30);

			//Assert
			Assert.AreEqual(value.ToString(), _workspaceNameChange);

			//Cleanup
			_sql = "";
			DeleteWorkspace(_client, _workspaceId);
		}

		[Test]
		public void ExecuteSqlStatementAsDbDataReader_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DbDataReader value = sut.ExecuteSqlStatementAsDbDataReader(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementGetSecondDataTable_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[User]; SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DataTable value = sut.ExecuteSQLStatementGetSecondDataTable(_sql, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementAsReader_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			SqlDataReader value = sut.ExecuteSQLStatementAsReader(_sql, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}


		[Test]
		public void ExecuteParameterizedSQLStatementAsReader_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			SqlDataReader value = sut.ExecuteParameterizedSQLStatementAsReader(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementAsEnumerable_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT [ArtifactID], [Name] FROM [EDDS].[eddsdbo].[Case]");

			//Act
			IEnumerable<string> value = sut.ExecuteSQLStatementAsEnumerable<string>(_sql, ConvertToString, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		private string ConvertToString(SqlDataReader sqlDataReader)
		{
			string returnString = sqlDataReader.GetInt32(0).ToString();
			return returnString;
		}

		[Test]
		public void ExecuteProcedureAsReader_Valid_sqlstring()
		{
			//Arrange
			//Act
			SqlParameter param1 = new SqlParameter();
			param1.ParameterName = "@UserID";
			param1.SqlDbType = SqlDbType.Int;
			param1.Direction = ParameterDirection.Input;
			param1.Value = 9;


			SqlParameter param2 = new SqlParameter();
			param2.ParameterName = "@ArtifactID";
			param2.SqlDbType = SqlDbType.Int;
			param2.Direction = ParameterDirection.Input;
			param2.Value = 9;

			DbDataReader dbDataReader = sut.ExecuteProcedureAsReader("GetUserPermissions", new List<SqlParameter> { param1, param2 });

			//Assert
			Assert.IsNotNull(dbDataReader);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlStatementAsDataSet_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DataSet dt = sut.ExecuteSqlStatementAsDataSet(_sql, null, 30);

			//Assert
			Assert.IsNotNull(dt);

			//Cleanup
			_sql = "";
		}


	    [Test]
	    public void ExecuteSqlBulkCopy_Valid()
	    {
	        //Arrange
	       //Create table TestTable
		    string tableName = "TestTable";
		    _sql = String.Format(@"CREATE TABLE TestTable (testID int,testCount int);");
		    sut.ExecuteSqlStatementAsDataSet(_sql, null, 30);	    

			//Create Bulk Copy Parameters
			ISqlBulkCopyParameters bulkCopyParameters = new Relativity.API.Context.SqlBulkCopyParameters();
			SqlBulkCopyColumnMapping testID = new SqlBulkCopyColumnMapping("ID","testID");
		    bulkCopyParameters.ColumnMappings.Add(testID);
			SqlBulkCopyColumnMapping count = new SqlBulkCopyColumnMapping("Count" , "testCount");
		    bulkCopyParameters.ColumnMappings.Add(count);
		    bulkCopyParameters.DestinationTableName = tableName;

			//Act
			using (IDataReader reader = GetSampleDataReader())
	        {
	            sut.ExecuteSqlBulkCopy(reader, bulkCopyParameters);
            }

			//Cleanup
			// Delete testtable
		    string _sqldrop = String.Format(@"DROP TABLE TestTable;");
		    sut.ExecuteSqlStatementAsDataSet(_sqldrop, null, 30);
		}

		#endregion

		#region Helpers

		private IDataReader GetSampleDataReader()
	    {
	        var dt = new DataTable();
	        dt.Columns.Add("ID", typeof(int));
	        dt.Columns.Add("Count", typeof(int));
	        dt.Rows.Add(1, 2);
	        dt.Rows.Add(3, 4);
	        return dt.CreateDataReader();
	    }

		public IRSAPIClient GetRsapiClient()
		{
			try
			{
				IRSAPIClient proxy = new RSAPIClient(ServicesUri, new UsernamePasswordCredentials(_userName, _password));
				proxy.APIOptions.WorkspaceID = WorkspaceID;
				return proxy;
			}
			catch (Exception ex)
			{
				Console.WriteLine("Failed to connect to RSAPI. " + ex);
				throw;
			}
		}

		private Int32 CreateWorkspace()
		{
			try
			{
				_client.APIOptions.WorkspaceID = -1;
				// retry logic for workspace creation
				int j = 1;

				while (j < 5)
				{
					j++;
					try
					{
						Console.WriteLine("Creating workspace.....");

						_workspaceId =
						Helpers.ArtifactHelpers.CreateWorkspace(_client,_workspaceName,TEST_WORKSPACE_TEMPLATE_NAME);
						Console.WriteLine($"Workspace created [WorkspaceArtifactId= {_workspaceId}].....");
						j = 5;
					}
					catch (Exception e)
					{
						Console.WriteLine("Failed to create workspace, Retry now...");

						if (j != 5)
							continue;
						_client = null;
						throw new Exception(
							$"Failed to create workspace in the setup. Reset the Client to null\nError Message:\n{e.Message}.\nInner Exception Message:\n{e.InnerException.Message}.\nStrack trace:\n{e.StackTrace}.", e);
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error encountered while creating a new Workspace.", ex);
			}
			finally
			{
				Console.WriteLine($"Workspace created [WorkspaceArtifactId= {_workspaceId}].....");
			}
			return _workspaceId;
		}

		public static bool DeleteWorkspace(IRSAPIClient proxy, int workspaceID)
		{

			proxy.APIOptions.WorkspaceID = -1;
			try
			{
				//Create a Workspace Artifact and pass to the Delete method on the repository
				Workspace workspaceDTO = new kCura.Relativity.Client.DTOs.Workspace(workspaceID);
				proxy.Repositories.Workspace.DeleteSingle(workspaceID);

			}
			catch (Exception ex)
			{
				Console.WriteLine("An error occurred deleting the Workspace: {0}", ex.Message);
				return false;
			}
			return true;
		}

		#endregion
	}
}
