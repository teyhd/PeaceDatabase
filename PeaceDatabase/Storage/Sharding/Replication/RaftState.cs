namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Роль узла в Raft консенсусе.
/// </summary>
public enum RaftRole
{
    /// <summary>
    /// Ведомый узел - принимает heartbeat от лидера.
    /// </summary>
    Follower,

    /// <summary>
    /// Кандидат - участвует в выборах.
    /// </summary>
    Candidate,

    /// <summary>
    /// Лидер - отправляет heartbeat и обрабатывает запросы клиентов.
    /// </summary>
    Leader
}

/// <summary>
/// Потокобезопасное состояние Raft для одного узла.
/// Управляет термом, голосованием и ролью узла.
/// </summary>
public sealed class RaftState
{
    private readonly object _lock = new();
    private readonly string _nodeId;
    private readonly Random _random = new();

    private long _currentTerm;
    private string? _votedFor;
    private RaftRole _role = RaftRole.Follower;
    private DateTimeOffset _lastHeartbeat = DateTimeOffset.UtcNow;
    private string? _currentLeaderId;

    /// <summary>
    /// Создаёт новое состояние Raft.
    /// </summary>
    /// <param name="nodeId">Уникальный идентификатор этого узла</param>
    /// <param name="initialTerm">Начальный терм (обычно 0)</param>
    public RaftState(string nodeId, long initialTerm = 0)
    {
        _nodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        _currentTerm = initialTerm;
    }

    /// <summary>
    /// Текущий терм Raft.
    /// </summary>
    public long CurrentTerm
    {
        get { lock (_lock) return _currentTerm; }
    }

    /// <summary>
    /// За кого проголосовали в текущем терме (null = не голосовали).
    /// </summary>
    public string? VotedFor
    {
        get { lock (_lock) return _votedFor; }
    }

    /// <summary>
    /// Текущая роль узла.
    /// </summary>
    public RaftRole Role
    {
        get { lock (_lock) return _role; }
    }

    /// <summary>
    /// Время последнего heartbeat.
    /// </summary>
    public DateTimeOffset LastHeartbeat
    {
        get { lock (_lock) return _lastHeartbeat; }
    }

    /// <summary>
    /// ID текущего лидера (если известен).
    /// </summary>
    public string? CurrentLeaderId
    {
        get { lock (_lock) return _currentLeaderId; }
    }

    /// <summary>
    /// ID этого узла.
    /// </summary>
    public string NodeId => _nodeId;

    /// <summary>
    /// Является ли узел лидером.
    /// </summary>
    public bool IsLeader => Role == RaftRole.Leader;

    /// <summary>
    /// Является ли узел кандидатом.
    /// </summary>
    public bool IsCandidate => Role == RaftRole.Candidate;

    /// <summary>
    /// Является ли узел ведомым.
    /// </summary>
    public bool IsFollower => Role == RaftRole.Follower;

    /// <summary>
    /// Обновляет терм, если новый терм больше текущего.
    /// При обновлении терма узел становится follower и сбрасывает голос.
    /// </summary>
    /// <param name="newTerm">Новый терм</param>
    /// <returns>true, если терм был обновлён</returns>
    public bool UpdateTerm(long newTerm)
    {
        lock (_lock)
        {
            if (newTerm > _currentTerm)
            {
                _currentTerm = newTerm;
                _votedFor = null;
                _role = RaftRole.Follower;
                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Пытается проголосовать за кандидата.
    /// Голос дается если:
    /// 1. Терм кандидата >= текущего терма
    /// 2. Мы еще не голосовали ИЛИ уже голосовали за этого кандидата
    /// 3. Лог кандидата не менее актуален (candidateSeq >= mySeq)
    /// </summary>
    /// <param name="candidateTerm">Терм кандидата</param>
    /// <param name="candidateId">ID кандидата</param>
    /// <param name="candidateSeq">Последний seq кандидата</param>
    /// <param name="mySeq">Наш последний seq</param>
    /// <returns>true, если голос отдан</returns>
    public bool Vote(long candidateTerm, string candidateId, long candidateSeq, long mySeq)
    {
        lock (_lock)
        {
            // Если терм кандидата больше нашего - обновляем и голосуем
            if (candidateTerm > _currentTerm)
            {
                _currentTerm = candidateTerm;
                _votedFor = null;
                _role = RaftRole.Follower;
            }

            // Отклоняем если терм кандидата меньше нашего
            if (candidateTerm < _currentTerm)
                return false;

            // Отклоняем если мы уже голосовали за другого
            if (_votedFor != null && _votedFor != candidateId)
                return false;

            // Отклоняем если лог кандидата менее актуален
            if (candidateSeq < mySeq)
                return false;

            // Отдаём голос
            _votedFor = candidateId;
            ResetHeartbeatInternal();
            return true;
        }
    }

    /// <summary>
    /// Начинает выборы - переходит в состояние Candidate и инкрементирует терм.
    /// </summary>
    /// <returns>Новый терм для выборов</returns>
    public long StartElection()
    {
        lock (_lock)
        {
            _currentTerm++;
            _role = RaftRole.Candidate;
            _votedFor = _nodeId; // Голосуем за себя
            ResetHeartbeatInternal();
            return _currentTerm;
        }
    }

    /// <summary>
    /// Узел становится лидером.
    /// </summary>
    public void BecomeLeader()
    {
        lock (_lock)
        {
            _role = RaftRole.Leader;
            _currentLeaderId = _nodeId;
            ResetHeartbeatInternal();
        }
    }

    /// <summary>
    /// Узел становится ведомым с указанным термом.
    /// </summary>
    /// <param name="term">Терм нового лидера</param>
    /// <param name="leaderId">ID нового лидера (опционально)</param>
    public void BecomeFollower(long term, string? leaderId = null)
    {
        lock (_lock)
        {
            if (term > _currentTerm)
            {
                _currentTerm = term;
                _votedFor = null;
            }
            _role = RaftRole.Follower;
            if (leaderId != null)
                _currentLeaderId = leaderId;
            ResetHeartbeatInternal();
        }
    }

    /// <summary>
    /// Устанавливает ID текущего лидера (для term sync).
    /// </summary>
    /// <param name="leaderId">ID лидера</param>
    public void SetLeaderId(string? leaderId)
    {
        lock (_lock)
        {
            _currentLeaderId = leaderId;
        }
    }

    /// <summary>
    /// Обрабатывает heartbeat от лидера.
    /// </summary>
    /// <param name="term">Терм лидера</param>
    /// <param name="leaderId">ID лидера</param>
    /// <returns>true, если heartbeat принят (терм валиден)</returns>
    public bool ReceiveHeartbeat(long term, string leaderId)
    {
        lock (_lock)
        {
            // Отклоняем heartbeat от устаревшего лидера
            if (term < _currentTerm)
                return false;

            // Если терм больше - обновляем
            if (term > _currentTerm)
            {
                _currentTerm = term;
                _votedFor = null;
            }

            // Становимся follower если были candidate или leader
            _role = RaftRole.Follower;
            _currentLeaderId = leaderId;
            ResetHeartbeatInternal();
            return true;
        }
    }

    /// <summary>
    /// Сбрасывает таймер heartbeat.
    /// </summary>
    public void ResetHeartbeat()
    {
        lock (_lock)
        {
            ResetHeartbeatInternal();
        }
    }

    private void ResetHeartbeatInternal()
    {
        _lastHeartbeat = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Проверяет, истёк ли таймаут выборов.
    /// </summary>
    /// <param name="electionTimeoutMs">Таймаут в миллисекундах</param>
    /// <returns>true, если таймаут истёк</returns>
    public bool IsElectionTimeoutElapsed(int electionTimeoutMs)
    {
        lock (_lock)
        {
            var elapsed = (DateTimeOffset.UtcNow - _lastHeartbeat).TotalMilliseconds;
            return elapsed >= electionTimeoutMs;
        }
    }

    /// <summary>
    /// Генерирует случайный таймаут выборов в заданном диапазоне.
    /// </summary>
    public int GetRandomElectionTimeout(int minMs, int maxMs)
    {
        lock (_lock)
        {
            return _random.Next(minMs, maxMs + 1);
        }
    }

    /// <summary>
    /// Получает снимок текущего состояния.
    /// </summary>
    public RaftStateSnapshot GetSnapshot()
    {
        lock (_lock)
        {
            return new RaftStateSnapshot
            {
                NodeId = _nodeId,
                CurrentTerm = _currentTerm,
                VotedFor = _votedFor,
                Role = _role,
                LastHeartbeat = _lastHeartbeat,
                CurrentLeaderId = _currentLeaderId
            };
        }
    }
}

/// <summary>
/// Снимок состояния Raft для диагностики.
/// </summary>
public sealed class RaftStateSnapshot
{
    public string NodeId { get; init; } = string.Empty;
    public long CurrentTerm { get; init; }
    public string? VotedFor { get; init; }
    public RaftRole Role { get; init; }
    public DateTimeOffset LastHeartbeat { get; init; }
    public string? CurrentLeaderId { get; init; }
}

