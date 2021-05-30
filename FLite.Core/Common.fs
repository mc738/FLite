namespace FLite.Core

open System
open System.IO
open Microsoft.Data.Sqlite
open Microsoft.FSharp.Reflection
open FLite.Core.Utils

[<AutoOpen>]
module Types =

    /// A blob field.
    type BlobField =
        { Value: Stream }
        static member FromStream(stream: Stream) = { Value = stream }

        static member FromBytes(ms: MemoryStream) = BlobField.FromStream(ms)

        member blob.ToBytes() =
            match blob.Value with
            | :? MemoryStream -> (blob.Value :?> MemoryStream).ToArray()
            | _ ->
                use ms = new MemoryStream()
                blob.Value.CopyTo(ms)
                ms.ToArray()

    (*
    /// A json file, stored as a blob in the database.
    type JsonField =
        { Json: string  }

        static member FromStream(stream: Stream) =
            Utf8JsonReader(stream)

        static member Serialize<'T>(value: 'T) = { Json =  JsonSerializer.Serialize value }

        member json.Deserialize<'T>() =
            JsonSerializer.Deserialize<'T> json.Json



        //static member FromStream<'T>
    *)



    module private TypeHelpers =
        let getName<'T> = typeof<'T>.FullName

        let typeName (t: Type) = t.FullName

        let boolName = getName<bool>

        let uByteName = getName<uint8>

        let uShortName = getName<uint16>

        let uIntName = getName<uint32>

        let uLongName = getName<uint64>

        let byteName = getName<byte>

        let shortName = getName<int16>

        let intName = getName<int>

        let longName = getName<int64>

        let floatName = getName<float>

        let doubleName = getName<double>

        let decimalName = getName<decimal>

        let charName = getName<char>

        let timestampName = getName<DateTime>

        let uuidName = getName<Guid>

        let stringName = getName<string>

        let blobName = getName<BlobField>

    [<RequireQualifiedAccess>]
    /// An internal DU for representing supported types.
    type SupportedType =
        | Boolean
        | Byte
        | Char
        | Decimal
        | Double
        | Float
        | Int
        | Short
        | Long
        | String
        | DateTime
        | Guid
        | Blob
        //| Json of Type

        static member TryFromName(name: String) =
            match name with
            | t when t = TypeHelpers.boolName -> Ok SupportedType.Boolean
            | t when t = TypeHelpers.byteName -> Ok SupportedType.Byte
            | t when t = TypeHelpers.charName -> Ok SupportedType.Char
            | t when t = TypeHelpers.decimalName -> Ok SupportedType.Decimal
            | t when t = TypeHelpers.doubleName -> Ok SupportedType.Double
            | t when t = TypeHelpers.floatName -> Ok SupportedType.Float
            | t when t = TypeHelpers.intName -> Ok SupportedType.Int
            | t when t = TypeHelpers.shortName -> Ok SupportedType.Short
            | t when t = TypeHelpers.longName -> Ok SupportedType.Long
            | t when t = TypeHelpers.stringName -> Ok SupportedType.String
            | t when t = TypeHelpers.timestampName -> Ok SupportedType.DateTime
            | t when t = TypeHelpers.uuidName -> Ok SupportedType.Guid
            | t when t = TypeHelpers.blobName -> Ok SupportedType.Blob
            | _ -> Error $"Type `{name}` not supported."

        static member TryFromType(typeInfo: Type) =
            SupportedType.TryFromName(typeInfo.FullName)

        static member FromName(name: string) =
            match SupportedType.TryFromName name with
            | Ok st -> st
            | Error _ -> SupportedType.String

        static member FromType(typeInfo: Type) =
            SupportedType.FromName(typeInfo.FullName)

module Mapping =

    type MappedFieldAttribute(name: string) =

        inherit Attribute()

        member att.Name = name

    type MappedRecordAttribute(name: string) =

        inherit Attribute()

        member att.Name = name

    type FieldValue = { Index: int; Value: obj }

    type MappedField =
        { FieldName: string
          MappingName: string
          Index: int
          Type: SupportedType }

        member field.CreateValue(value: obj) = { Index = field.Index; Value = value }

    type MappedObject =
        { Fields: MappedField list
          Type: Type }

        static member Create<'T>() =
            let t = typeof<'T>

            let fields =
                t.GetProperties()
                |> List.ofSeq
                |> List.fold
                    (fun (acc, i) pi ->
                        let newAcc =
                            match System.Attribute.GetCustomAttribute(pi, typeof<MappedFieldAttribute>) with
                            | att when att <> null ->
                                let mfa = att :?> MappedFieldAttribute

                                // TODO check if supported type.

                                // TODO handle blobs and unhandled property types.

                                acc
                                @ [ { FieldName = pi.Name
                                      MappingName = mfa.Name
                                      Index = i
                                      Type = SupportedType.FromType(pi.PropertyType) } ]
                            | _ ->
                                acc
                                @ [ { FieldName = pi.Name
                                      MappingName = pi.Name.ToSnakeCase()
                                      Index = i
                                      Type = SupportedType.FromType(pi.PropertyType) } ]

                        (newAcc, i + 1))
                    ([], 0)
                |> fun (r, _) -> r

            { Fields = fields; Type = t }

        static member CreateNoAtt<'T>() =
            let t = typeof<'T>

            let fields =
                t.GetProperties()
                |> List.ofSeq
                |> List.mapi
                    (fun i pi ->
                        { FieldName = pi.Name
                          MappingName = pi.Name.ToSnakeCase()
                          Index = i
                          Type = SupportedType.FromType(pi.PropertyType) })

            { Fields = fields; Type = t }

        member map.GetIndexedMap() =
            map.Fields
            |> List.map (fun f -> f.Index, f)
            |> Map.ofList

        member map.GetNamedMap() =
            map.Fields
            |> List.map (fun f -> f.MappingName, f)
            |> Map.ofList

    type RecordBuilder() =

        static member Create<'T>(values: FieldValue list) =
            let t = typeof<'T>

            let v =
                values
                |> List.sortBy (fun v -> v.Index)
                |> List.map (fun v -> v.Value)
                |> Array.ofList

            let o = FSharpValue.MakeRecord(t, v)

            o :?> 'T

module private QueryHelpers =

    open Mapping

    let mapParameters<'T> (mappedObj: MappedObject) (parameters: 'T) =
        mappedObj.Fields
        |> List.sortBy (fun p -> p.Index)
        |> List.map
            (fun f ->
                let v =
                    mappedObj
                        .Type
                        .GetProperty(f.FieldName)
                        .GetValue(parameters)

                f.MappingName, v)
        |> Map.ofList

    let mapResults<'T> (mappedObj: MappedObject) (reader: SqliteDataReader) =
        [ while reader.Read() do
              mappedObj.Fields
              |> List.map
                  (fun f ->
                      let o = reader.GetOrdinal(f.MappingName)

                      let value =
                          match f.Type with
                          | SupportedType.Boolean -> reader.GetBoolean(o) :> obj
                          | SupportedType.Byte -> reader.GetByte(o) :> obj
                          | SupportedType.Char -> reader.GetChar(o) :> obj
                          | SupportedType.Decimal -> reader.GetDecimal(o) :> obj
                          | SupportedType.Double -> reader.GetDouble(o) :> obj
                          | SupportedType.Float -> reader.GetFloat(o) :> obj
                          | SupportedType.Int -> reader.GetInt32(o) :> obj
                          | SupportedType.Short -> reader.GetInt16(o) :> obj
                          | SupportedType.Long -> reader.GetInt64(o) :> obj
                          | SupportedType.String -> reader.GetString(o) :> obj
                          | SupportedType.DateTime -> reader.GetDateTime(o) :> obj
                          | SupportedType.Guid -> reader.GetGuid(o) :> obj
                          | SupportedType.Blob -> BlobField.FromStream(reader.GetStream(o)) :> obj
                      //| SupportedType.Json -> JsonField.Deserialize(reader.GetStream(o)) :> obj


                      { Index = f.Index; Value = value })
              |> (fun v -> RecordBuilder.Create<'T> v) ]

    let noParam (connection: SqliteConnection) (sql: string) (transaction: SqliteTransaction option) =

        connection.Open()
        use comm =
            match transaction with
            | Some t -> new SqliteCommand(sql, connection, t)
            | None -> new SqliteCommand(sql, connection)
        comm

    let prepare<'P> (connection: SqliteConnection) (sql: string) (mappedObj: MappedObject) (parameters: 'P) (transaction: SqliteTransaction option) =
        connection.Open()
        
        use comm =
            match transaction with
                | Some t -> new SqliteCommand(sql, connection, t)
                | None -> new SqliteCommand(sql, connection)
       
        parameters
        |> mapParameters<'P> mappedObj
        |> Map.map (fun k v -> comm.Parameters.AddWithValue(k, v))
        |> ignore

        comm.Prepare()
        comm


    let rawNonQuery (connection: SqliteConnection) (sql: string) (transaction: SqliteTransaction option) =
        let comm =  noParam connection sql transaction

        comm.ExecuteNonQuery()


    let verbatimNonQuery<'P> (connection: SqliteConnection) (sql: string) (parameters: 'P) (transaction: SqliteTransaction option)  =
        let mappedObj = MappedObject.Create<'P>()
        let comm = prepare connection sql mappedObj parameters transaction
        comm.ExecuteNonQuery()
    
    let create<'T> (tableName: string) (connection: SqliteConnection) (transaction: SqliteTransaction option) =
        let mappedObj = MappedObject.Create<'T>()

        let columns =
            mappedObj.Fields
            |> List.sortBy (fun p -> p.Index)
            |> List.map
                (fun f ->
                    let template (colType: string) = $"{f.MappingName} {colType}"

                    let blobField =
                        $"{f.MappingName} BLOB, {f.MappingName}_sha256_hash TEXT"

                    match f.Type with
                    | SupportedType.Boolean -> template "INTEGER"
                    | SupportedType.Byte -> template "INTEGER"
                    | SupportedType.Int -> template "INTEGER"
                    | SupportedType.Short -> template "INTEGER"
                    | SupportedType.Long -> template "INTEGER"
                    | SupportedType.Double -> template "REAL"
                    | SupportedType.Float -> template "REAL"
                    | SupportedType.Decimal -> template "REAL"
                    | SupportedType.Char -> template "TEXT"
                    | SupportedType.String -> template "TEXT"
                    | SupportedType.DateTime -> template "TEXT"
                    | SupportedType.Guid -> template "TEXT"
                    | SupportedType.Blob -> template "BLOB")
        //| SupportedType.Json -> template "BLOB")

        let columnsString = String.Join(',', columns)

        let sql =
            $"""
        CREATE TABLE {tableName} ({columnsString});
        """

        let comm = noParam connection sql transaction

        comm.ExecuteNonQuery()

    let selectAll<'T> (tableName: string) (connection: SqliteConnection) (transaction: SqliteTransaction option) =
        let mappedObj = MappedObject.Create<'T>()

        let fields =
            mappedObj.Fields
            |> List.sortBy (fun p -> p.Index)
            |> List.map (fun f -> f.MappingName)

        let fieldsString = String.Join(',', fields)

        let sql =
            $"""
        SELECT {fieldsString}
        FROM {tableName}
        """

        let comm = noParam connection sql transaction

        use reader = comm.ExecuteReader()

        mapResults<'T> mappedObj reader

    let select<'T, 'P> (sql: string) (connection: SqliteConnection) (parameters: 'P) (transaction: SqliteTransaction option) =
        let tMappedObj = MappedObject.Create<'T>()
        let pMappedObj = MappedObject.Create<'P>()

        let comm =
            prepare connection sql pMappedObj parameters transaction

        use reader = comm.ExecuteReader()

        mapResults<'T> tMappedObj reader

    let selectSql<'T> (sql: string) (connection: SqliteConnection) (transaction: SqliteTransaction option) =
        let tMappedObj = MappedObject.Create<'T>()

        let comm = noParam connection sql transaction

        use reader = comm.ExecuteReader()

        mapResults<'T> tMappedObj reader

    
    [<RequireQualifiedAccess>]
    /// Special handling is needed for `INSERT` query to accommodate blobs.
    /// This module aims to wrap as much of that up to in one place.
    module private Insert =

        type InsertBlobCallback = { ColumnName: string; Data: Stream }

        /// Create an insert query and return the sql and a list of `InsertBlobCallback`'s.
        let createQuery<'T> (tableName: string) (mappedObj: MappedObject) (data: 'T) =
            let (fieldNames, parameterNames, blobCallbacks) =
                mappedObj.Fields
                |> List.fold
                    (fun (fn, pn, cb) f ->

                        match f.Type with
                        | SupportedType.Blob ->
                            // Get the blob.
                            let stream =
                                (mappedObj
                                    .Type
                                    .GetProperty(f.FieldName)
                                    .GetValue(data)
                                :?> BlobField)
                                    .Value

                            let callback =
                                { ColumnName = f.MappingName
                                  Data = stream }

                            (fn @ [ f.MappingName ], pn @ [ $"ZEROBLOB({stream.Length})" ], cb @ [ callback ])
                        | _ -> (fn @ [ f.MappingName ], pn @ [ $"@{f.MappingName}" ], cb))
                    ([], [], [])

            let fields = String.Join(',', fieldNames)
            let parameters = String.Join(',', parameterNames)

            let sql =
                $"""
            INSERT INTO {tableName} ({fields})
            VALUES ({parameters});
            SELECT last_insert_rowid();
            """

            (sql, blobCallbacks)

        /// Prepare the `INSERT` query and return a `SqliteCommand` ready for execution.
        /// `BlobField` types will be skipped over, due to being handled seperately.
        let prepareQuery<'P> (connection: SqliteConnection) (sql: string) (mappedObj: MappedObject) (parameters: 'P) (transaction: SqliteTransaction option) =
            connection.Open()

            
            use comm =
                match transaction with
                | Some t -> new SqliteCommand(sql, connection, t)
                | None -> new SqliteCommand(sql, connection)

            mappedObj.Fields
            |> List.sortBy (fun p -> p.Index)
            |> List.fold
                (fun acc f ->
                    match f.Type with
                    | SupportedType.Blob -> acc // Skip blob types, they will be handled with `BlobCallBacks`.
                    | _ ->
                        acc
                        @ [ f.MappingName,
                            mappedObj
                                .Type
                                .GetProperty(f.FieldName)
                                .GetValue(parameters) ])
                []
            |> Map.ofList
            |> Map.map (fun k v -> comm.Parameters.AddWithValue(k, v))
            |> ignore

            comm.Prepare()
            comm

        let handleBlobCallbacks
            (connection: SqliteConnection)
            (tableName: string)
            (callbacks: InsertBlobCallback list)
            rowId
            =
            callbacks
            |> List.map
                (fun cb ->
                    use writeStream =
                        new SqliteBlob(connection, tableName, cb.ColumnName, rowId)

                    cb.Data.CopyTo(writeStream))
            |> ignore

    let insert<'T> (tableName: string) (connection: SqliteConnection) (data: 'T) (transaction: SqliteTransaction option) =
        let mappedObj = MappedObject.Create<'T>()

        let (sql, callbacks) =
            Insert.createQuery tableName mappedObj data

        // Get the last inserted id.
        let comm =
            Insert.prepareQuery connection sql mappedObj data transaction

        let rowId = comm.ExecuteScalar() :?> int64

        Insert.handleBlobCallbacks connection tableName callbacks rowId


type QueryHandler(connection: SqliteConnection, transaction: SqliteTransaction option) =

    static member Create(path: string) =
        printfn "Creating database '%s'." path
        File.WriteAllBytes(path, [||])

        use conn =
            new SqliteConnection($"Data Source={path}")

        QueryHandler(conn, None)

    static member Open(path: string) =
        printfn "Connection to database '%s'." path

        use conn =
            new SqliteConnection($"Data Source={path}")

        QueryHandler(conn, None)

    //let test = Queries.createVerbatimSelectQuery<string, int> "SELECT id from table"

    //let r = test.Execute(connection, 1)

    member handler.Select<'T>(tableName) =
        QueryHelpers.selectAll<'T> tableName connection transaction

    /// Select data based on a verbatim sql and parameters.
    member handler.SelectVerbatim<'T, 'P>(sql, parameters) =
        QueryHelpers.select<'T, 'P> sql connection parameters transaction

    member handler.SelectSql<'T>(sql) =
        QueryHelpers.selectSql<'T>(sql) connection transaction
    
    member handler.SelectSingle<'T>(tableName) = handler.Select<'T>(tableName).Head

    member handler.SelectSingleVerbatim<'T, 'P>(sql: string, parameters: 'P) =
        handler
            .SelectVerbatim<'T, 'P>(
                sql,
                parameters
            )
            .Head

    /// Execute a create table query based on a generic record.
    member handler.CreateTable<'T>(tableName: string) =
        QueryHelpers.create<'T> tableName connection transaction

    /// Execute a raw sql non query. What is passed as a parameters is what will be executed.
    /// WARNING: do not used with untrusted input.
    member handler.ExecuteSqlNonQuery(sql: string) =
        QueryHelpers.rawNonQuery connection sql transaction

    /// Execute a verbatim non query. The parameters passed will be mapped to the sql query.
    member handler.ExecuteVerbatimNonQuery<'P>(sql: string, parameters: 'P) =
        QueryHelpers.verbatimNonQuery connection sql parameters transaction
    
    /// Execute an insert query.
    member handler.Insert<'T>(tableName: string, value: 'T) =
        QueryHelpers.insert<'T> tableName connection value transaction

    /// Execute a collection of insert queries.
    member handler.InsertList<'T>(tableName: string, values: 'T list) =
        values
        |> List.map (fun v -> handler.Insert<'T>(tableName, v))
        |> ignore


    /// Execute a collection of commands in a transaction.
    /// While a transaction is active on a connection non transaction commands can not be executed.
    /// This is no check for this for this is not thread safe.
    /// Also be warned, this use general error handling so an exception will roll the transaction back.
    member handler.ExecuteInTransaction<'R>(transactionFn: QueryHandler -> 'R) =
        connection.Open()
        use transaction = connection.BeginTransaction()
        
        let qh = QueryHandler(connection, Some transaction)
        
        try
            let r = transactionFn qh
            transaction.Commit()
            Ok r
        with
        | _ ->
            transaction.Rollback()
            Error "Could not complete transaction"