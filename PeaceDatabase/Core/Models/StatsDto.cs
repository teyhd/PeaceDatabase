namespace PeaceDatabase.Core.Models
{
    public sealed class StatsDto
    {
        public string Db { get; set; } = "";
        public int Seq { get; set; }
        public int DocsTotal { get; set; }
        public int DocsAlive { get; set; }
        public int DocsDeleted { get; set; }
        public int EqIndexFields { get; set; }
        public int TagIndexCount { get; set; }
        public int FullTextTokens { get; set; }
    }
}
