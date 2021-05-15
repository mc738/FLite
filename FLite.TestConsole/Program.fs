// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp

open System
open System.IO
open System.Text
open FLite.Core
open FLite.Core.Mapping

type Foo = {
    [<MappedField("id")>] Id: Guid
    [<MappedField("name")>] Name: string
    [<MappedField("blob_1")>]  Blob1: BlobField
    [<MappedField("blob_2")>]  Blob2: BlobField
}


type Bar = {
    [<MappedField("id")>] Id: Guid
    [<MappedField("created_on")>] CreatedOn: DateTime
}

type Baz = {
    [<MappedField("id")>] Id: Guid
    [<MappedField("name")>] Name: string
    [<MappedField("blob_1")>]  Blob1: BlobField
    [<MappedField("blob_2")>]  Blob2: BlobField
    [<MappedField("created_on")>] CreatedOn: DateTime
}

     
type BazQuery = {
        [<MappedField("id")>] Id: Guid
}

// Define a function to construct a message to print
let from whom =
    sprintf "from %s" whom

[<EntryPoint>]
let main argv =
    use blob1 = new MemoryStream(Encoding.UTF8.GetBytes("Hello, World!"))
    let blob2 = new MemoryStream(Encoding.UTF8.GetBytes("More data!"))

    let id = Guid.NewGuid()

    let qh = QueryHandler.Create($"/home/max/Data/FLiteTests/{DateTime.Now:yyyyMMddHHmmss}.db")

    let foo = {
        Id = id
        Name = "Test"
        Blob1 = BlobField.FromBytes blob1
        Blob2 = BlobField.FromBytes blob2
    }

    let bar = {
        Id = id
        CreatedOn = DateTime.UtcNow
    }

    let sql = """
    SELECT f.id, f.name, f.blob_1, f.blob_2, b.created_on
    FROM foo f
    JOIN bar b ON f.id = b.id
    WHERE f.id = @id
    """

    qh.ExecuteInTransaction<unit>(fun t ->
        t.CreateTable<Foo>("foo") |> ignore
        t.CreateTable<Bar>("bar") |> ignore    
        t.Insert("foo", foo)
        t.Insert("bar", bar)) |> ignore
 
    let baz = qh.SelectSingleVerbatim<Baz, BazQuery>(sql, { Id = id })
    
    printfn "Baz: %A" baz
    0 // return an integer exit code