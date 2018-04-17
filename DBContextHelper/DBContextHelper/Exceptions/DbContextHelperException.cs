namespace DbContextHelper.Exceptions
{
	[System.Serializable]
	public class DbContextHelperException : System.Exception
	{
		public DbContextHelperException()
				: base()
		{
		}

		public DbContextHelperException(string message)
				: base(message)
		{
		}

		public DbContextHelperException(string message, System.Exception inner)
				: base(message, inner)
		{
		}

		// A constructor is needed for serialization when an
		// exception propagates from a remoting server to the client. 
		protected DbContextHelperException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context)
		{
		}
	}
}
