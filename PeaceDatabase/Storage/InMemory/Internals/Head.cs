namespace PeaceDatabase.Storage.InMemory.Internals
{
    /// <summary>
    /// "Голова" документа: текущая активная ревизия + флажок удаления.
    /// </summary>
    internal sealed class Head
    {
        public string Rev { get; set; } = string.Empty;
        public bool Deleted { get; set; }
    }
}
