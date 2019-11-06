using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;

public class TableStorageWriter
{
    private const int _batchSize = 100;
    private readonly ConcurrentQueue<Tuple<ITableEntity, TableOperation>> _operations;
    private readonly CloudStorageAccount _storageAccount;
    private readonly string _tableName;

    public TableStorageWriter(string connectionStr, string tableName)
    {
        this._tableName = tableName;

        _storageAccount = CloudStorageAccount.Parse(connectionStr);

        var tableReference = MakeTableReference();

        tableReference.CreateIfNotExists();

        _operations = new ConcurrentQueue<Tuple<ITableEntity, TableOperation>>();
    }

    private CloudTable MakeTableReference()
    {
        var tableClient = _storageAccount.CreateCloudTableClient();
        var tableReference = tableClient.GetTableReference(_tableName);
        return tableReference;
    }

    public decimal OutstandingOperations
    {
        get { return _operations.Count; }
    }

    public IEnumerable<ITableEntity> GetAllEntities()
    {
        var entities = new List<ITableEntity>();
        var table = MakeTableReference();
        TableContinuationToken token = null;
        do
        {
            var queryResult = table.ExecuteQuerySegmented(new TableQuery(), token);
            entities.AddRange(queryResult.Results);
            token = queryResult.ContinuationToken;
        } while (token != null);

        return entities;
    }

    public void Insert<TEntity>(TEntity entity)
        where TEntity : ITableEntity
    {
        var e = new Tuple<ITableEntity, TableOperation>
            (entity,
                TableOperation.Insert(entity));
        _operations.Enqueue(e);
    }

    public void Delete<TEntity>(TEntity entity)
        where TEntity : ITableEntity
    {
        var e = new Tuple<ITableEntity, TableOperation>
            (entity,
                TableOperation.Delete(entity));
        _operations.Enqueue(e);
    }

    public void InsertOrMerge<TEntity>(TEntity entity)
        where TEntity : ITableEntity
    {
        var e = new Tuple<ITableEntity, TableOperation>
            (entity,
                TableOperation.InsertOrMerge(entity));
        _operations.Enqueue(e);
    }

    public void InsertOrReplace<TEntity>(TEntity entity)
        where TEntity : ITableEntity
    {
        var e = new Tuple<ITableEntity, TableOperation>
            (entity,
                TableOperation.InsertOrReplace(entity));
        _operations.Enqueue(e);
    }

    public void Merge<TEntity>(TEntity entity)
        where TEntity : ITableEntity
    {
        var e = new Tuple<ITableEntity, TableOperation>
            (entity,
                TableOperation.Merge(entity));
        _operations.Enqueue(e);
    }

    public void Replace<TEntity>(TEntity entity)
        where TEntity : ITableEntity
    {
        var e = new Tuple<ITableEntity, TableOperation>
            (entity,
                TableOperation.Replace(entity));
        _operations.Enqueue(e);
    }

    public void Execute()
    {
        var count = _operations.Count;
        var toExecute = new List<Tuple<ITableEntity, TableOperation>>();
        for (var index = 0; index < count; index++)
        {
            Tuple<ITableEntity, TableOperation> operation;
            _operations.TryDequeue(out operation);
            if (operation != null)
                toExecute.Add(operation);
        }

        toExecute
           .GroupBy(tuple => tuple.Item1.PartitionKey)
           .ToList()
           .ForEach(g =>
           {
               var opreations = g.ToList();

               var batch = 0;
               var operationBatch = GetOperations(opreations, batch);

               while (operationBatch.Any())
               {
                   var tableBatchOperation = MakeBatchOperation(operationBatch);

                   ExecuteBatchWithRetries(tableBatchOperation);

                   batch++;
                   operationBatch = GetOperations(opreations, batch);
               }
           });
    }

    private void ExecuteBatchWithRetries(TableBatchOperation tableBatchOperation)
    {
        var tableRequestOptions = MakeTableRequestOptions();

        var tableReference = MakeTableReference();

        tableReference.ExecuteBatch(tableBatchOperation, tableRequestOptions);
    }

    private static TableRequestOptions MakeTableRequestOptions()
    {
        return new TableRequestOptions
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromMilliseconds(2),
                                                   100)
        };
    }

    private static TableBatchOperation MakeBatchOperation(
        List<Tuple<ITableEntity, TableOperation>> operationsToExecute)
    {
        var tableBatchOperation = new TableBatchOperation();
        operationsToExecute.ForEach(tuple => tableBatchOperation.Add(tuple.Item2));
        return tableBatchOperation;
    }

    private static List<Tuple<ITableEntity, TableOperation>> GetOperations(
        IEnumerable<Tuple<ITableEntity, TableOperation>> opreations,
        int batch)
    {
        return opreations
            .Skip(batch * _batchSize)
            .Take(_batchSize)
            .ToList();
    }
}