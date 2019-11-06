using System;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableCleaner
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter the connection string:");
            var connStr = Console.ReadLine();
            CloudStorageAccount storageAccount = null;
            try
            {
                storageAccount = CloudStorageAccount.Parse(connStr);
            }
            catch(Exception ex)
            {
                Console.WriteLine("Invalid connection string : " + connStr + ex.Message);
                return;
            }

            Console.WriteLine("Enter the table name:");
            var tableName = Console.ReadLine();

            TableStorageWriter tableWriter = new TableStorageWriter(connStr, tableName);

            var entities = tableWriter.GetAllEntities();
            foreach(var entity in entities)
            {
                tableWriter.Delete(entity);
            }
            tableWriter.Execute();
            
            Console.WriteLine("The azure table {0} has been cleared.", tableName)      
        }
    }
}
