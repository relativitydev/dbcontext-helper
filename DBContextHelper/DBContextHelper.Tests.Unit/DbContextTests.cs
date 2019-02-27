using DBContextHelper;
using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using Relativity.API;
using System.Data;

namespace DBContextHelper.Tests.Unit
{
	[TestFixture]
	public class DbContextTests
	{
		private string _database = "testDatabase";
		private string _server = "testServer";
		private string _login = "username";
		private string _password = "password";
		public Mock<IDBContext> MockDBContext;
		private string _sql;

		public DbContext sut { get; set; }

		
		[SetUp]
		public void Setup()
		{
		 sut = new DbContext(_server, _database, _login, _password);
		 MockDBContext = new Mock<IDBContext>();
		}

		[Test]
		public void ExecuteSqlStatementAsDataTable_null_sqlstring()
		{
		    DataTable dt = sut.ExecuteSqlStatementAsDataTable(_sql, 30, null);
			
		}


	}

}
