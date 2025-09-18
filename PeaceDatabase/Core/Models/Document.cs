namespace PeaceDatabase.Core.Models;

public class Document
{
    public string Id { get; set; } = string.Empty;
    public string Rev { get; set; } = string.Empty;
    public bool Deleted { get; set; } = false;
    public Dictionary<string, object?> Body { get; set; } = new();
}
