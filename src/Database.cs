using System.Data.Common;
using TL;
using Chat = WTelegram.Types.Chat;
using Update = WTelegram.Types.Update;
using User = WTelegram.Types.User;

namespace WTelegram;

internal partial class Database : IDisposable
{
	private readonly Func<string?, (DbConnection, string)> _connectionFactory;
	private readonly string[] _cmd = new string[DefaultSqlCommands[0].Length];
	private readonly Bot.State _state;

	public Database(Func<string?, (DbConnection, string)> connectionFunc, string[] sqlCommands, Bot.State state)
	{
        _connectionFactory = connectionFunc;
		if (sqlCommands.Length != DefaultSqlCommands[0].Length)
			throw new ArgumentException($"Expected {DefaultSqlCommands[0].Length} SQL commands", nameof(sqlCommands));
		_state = state;
		for (int i = 0; i < sqlCommands.Length; i++)
		{
            _cmd[i] = sqlCommands[i];
		}

        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(DbSetup, cmd);
        cmd.ExecuteNonQuery();
	}

    private DbConnection _connectionFunc()
    {
        return _connectionFactory(null).Item1;
    }

    public void InitCmd(int i, DbCommand command)
    {
        command.CommandText = _connectionFactory(_cmd[i]).Item2;
        string defCmd = DefaultSqlCommands[0][i];
        for (int at = 0; (at = defCmd.IndexOf('@', at + 1)) > 0;)
        {
            var end = defCmd.IndexOfAny([',', ' ', ')', ';'], at + 1);
            var param = command.CreateParameter();
            param.ParameterName = defCmd[at..end];
            if (!command.Parameters.Contains(param.ParameterName))
                command.Parameters.Add(param);
        }
    }

    public void Dispose()
	{
		// foreach (var cmd in _cmd) cmd.Dispose();
		// _connection.Dispose();
	}

	internal Stream LoadSessionState()
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(LoadSession, cmd);

		//using (var reader = _cmd[LoadSession].ExecuteReader())
        using (var reader = cmd.ExecuteReader())
			if (reader.Read())
			{
				_state.SessionData = reader.GetValue(0) as byte[];
				_state.LastUpdateId = reader.GetInt32(1);
				_state.AllowedUpdates = reader.GetInt32(2);
			}
		return new SessionStore(_state.SessionData, SaveSessionState);
	}

	internal void SaveSessionState(byte[]? sessionData = null)
	{
		if (sessionData != null) _state.SessionData = sessionData;
		// var cmd = _cmd[SaveSession];

        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(SaveSession, cmd);

		cmd.Parameters[0].Value = _state.SessionData;
		cmd.Parameters[1].Value = _state.LastUpdateId;
		cmd.Parameters[2].Value = _state.AllowedUpdates;
		cmd.ExecuteSave();
	}

	class SessionStore(byte[]? _data, Action<byte[]> save) : Stream
	{
		private int _dataLen = _data?.Length ?? 0;
		private DateTime _lastWrite;
		private Task? _delayedWrite;

		protected override void Dispose(bool disposing) => _delayedWrite?.Wait();

		public override int Read(byte[] buffer, int offset, int count)
		{
			Array.Copy(_data!, 0, buffer, offset, count);
			return count;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			_data = buffer; _dataLen = count;
			if (_delayedWrite != null) return;
			var left = 1000 - (int)(DateTime.UtcNow - _lastWrite).TotalMilliseconds;
			if (left < 0)
			{
				save(buffer[offset..(offset + count)]);
				_lastWrite = DateTime.UtcNow;
			}
			else
				_delayedWrite = Task.Delay(left).ContinueWith(t => { lock (this) { _delayedWrite = null; Write(_data, 0, _dataLen); } });
		}

		public override long Length => _dataLen;
		public override long Position { get => 0; set { } }
		public override bool CanSeek => false;
		public override bool CanRead => true;
		public override bool CanWrite => true;
		public override long Seek(long offset, SeekOrigin origin) => 0;
		public override void SetLength(long value) { }
		public override void Flush() { }
	}

	internal IEnumerable<(int id, TL.Update update)> LoadTLUpdates()
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(LoadUpdates, cmd);

		using var reader = cmd.ExecuteReader();
		while (reader.Read())
			using (var breader = new BinaryReader(reader.GetStream(1)))
				yield return (reader.GetInt32(0), (TL.Update)breader.ReadTLObject(0));
	}

	internal void SaveTLUpdates(IEnumerable<Update> updates)
	{
        {
            using var conn1 = _connectionFunc();
            conn1.Open();
            using var cmd1 = conn1.CreateCommand();
            InitCmd(DelUpdates, cmd1);
            cmd1.ExecuteSave();
        }

        // _cmd[DelUpdates].ExecuteSave();
		//var cmd = _cmd[SaveUpdates];
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(SaveUpdates, cmd);
		using var memStream = new MemoryStream(1024);
		foreach (var botUpdate in updates)
		{
			if (botUpdate.TLUpdate == null) continue;
			memStream.SetLength(0);
			using (var writer = new BinaryWriter(memStream, System.Text.Encoding.UTF8, leaveOpen: true))
				botUpdate.TLUpdate.WriteTL(writer);
			cmd.Parameters[0].Value = botUpdate.Id;
			cmd.Parameters[1].Value = memStream.ToArray();
			cmd.ExecuteSave();
		}
	}

	internal Dictionary<long, UpdateManager.MBoxState> LoadMBoxStates()
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(LoadMBox, cmd);

        //using var reader = _cmd[LoadMBox].ExecuteReader();
		using var reader = cmd.ExecuteReader();
		var result = new Dictionary<long, UpdateManager.MBoxState>();
		while (reader.Read())
			result[reader.GetInt64(0)] = new() { pts = reader.GetInt32(1), access_hash = reader.GetInt64(2) };
		return result;
	}

	internal void SaveMBoxStates(IReadOnlyDictionary<long, UpdateManager.MBoxState> state)
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(SaveMBox, cmd);

		//var cmd = _cmd[SaveMBox];
		foreach (var mboxState in state)
		{
			cmd.Parameters[0].Value = mboxState.Key;
			cmd.Parameters[1].Value = mboxState.Value.pts;
			cmd.Parameters[2].Value = mboxState.Value.access_hash;
			cmd.ExecuteSave();
		}
	}

	internal void GetTables(out CachedTable<User> users, out CachedTable<Chat> chats)
	{
		users = new CachedTable<User>(DoLoadUser, DoSaveUser);
		chats = new CachedTable<Chat>(DoLoadChat, DoSaveChat);
	}

	private User? DoLoadUser(long id)
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(LoadUser, cmd);

		//_cmd[LoadUser].Parameters[0].Value = id;
        cmd.Parameters[0].Value = id;
		//using var reader = _cmd[LoadUser].ExecuteReader();
        using var reader = cmd.ExecuteReader();
		if (!reader.Read()) return null;
		var flags = reader.GetInt32(1);
		return new User
		{
			Id = id,
			AccessHash = reader.GetInt64(0),
			FirstName = reader.GetString(2),
			LastName = reader.GetString(3).NullIfEmpty(),
			Username = reader.GetString(4).NullIfEmpty(),
			LanguageCode = reader.GetString(5).NullIfEmpty(),
			IsBot = (flags & 1) != 0, IsPremium = (flags & 2) != 0, AddedToAttachmentMenu = (flags & 4) != 0,
			CanJoinGroups = (flags & 8) != 0, CanReadAllGroupMessages = (flags & 16) != 0, SupportsInlineQueries = (flags & 32) != 0,
			CanConnectToBusiness = (flags & 64) != 0, HasMainWebApp = (flags & 128) != 0, HasTopicsEnabled = (flags & 256) != 0,
			AllowsUsersToCreateTopics = (flags & 512) != 0, CanManageBots = (flags & 1024) != 0, SupportsGuestQueries = (flags & 2048) != 0,
			SupportsJoinRequestQueries = (flags & 4096) != 0,
		};
	}

	private void DoSaveUser(User user)
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(SaveUser, cmd);

		//var param = _cmd[SaveUser].Parameters;
        var param = cmd.Parameters;
		param[0].Value = user.Id;
		param[1].Value = user.AccessHash;
		param[2].Value = (user.IsBot ? 1 : 0) | (user.IsPremium ? 2 : 0) | (user.AddedToAttachmentMenu ? 4 : 0)
			| (user.CanJoinGroups ? 8 : 0) | (user.CanReadAllGroupMessages ? 16 : 0) | (user.SupportsInlineQueries ? 32 : 0)
			| (user.CanConnectToBusiness ? 64 : 0) | (user.HasMainWebApp ? 128 : 0) | (user.HasTopicsEnabled ? 256 : 0)
			| (user.AllowsUsersToCreateTopics ? 512 : 0) | (user.CanManageBots ? 1024 : 0) | (user.SupportsGuestQueries ? 2048 : 0)
			| (user.SupportsJoinRequestQueries ? 4096 : 0);
		param[3].Value = user.FirstName;
		param[4].Value = user.LastName ?? "";
		param[5].Value = user.Username ?? "";
		param[6].Value = user.LanguageCode ?? "";
		// _cmd[SaveUser].ExecuteSave();
        cmd.ExecuteSave();
	}

	private Chat? DoLoadChat(long id)
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(LoadChat, cmd);

		//_cmd[LoadChat].Parameters[0].Value = id;
		//using var reader = _cmd[LoadChat].ExecuteReader();
        cmd.Parameters[0].Value = id;
        using var reader = cmd.ExecuteReader();
		if (!reader.Read()) return null;
		var flags = reader.GetInt32(1);
		var type = (ChatType)reader.GetInt32(5);
		var firstName = reader.GetString(2);
		return new Chat
		{
			Id = type == ChatType.Group ? -id : Bot.ZERO_CHANNEL_ID - id,
			AccessHash = reader.GetInt64(0),
			Type = type,
			Title = type == ChatType.Private ? null : firstName,
			FirstName = type == ChatType.Private ? firstName : null,
			LastName = type == ChatType.Private ? reader.GetString(3).NullIfEmpty() : null,
			Username = reader.GetString(4).NullIfEmpty(),
			IsForum = (flags & 1) != 0,
		};
	}

	private void DoSaveChat(Chat chat)
	{
        using var conn = _connectionFunc();
        conn.Open();
        using var cmd = conn.CreateCommand();
        InitCmd(SaveChat, cmd);

		// var param = _cmd[SaveChat].Parameters;
        var param = cmd.Parameters;
		param[0].Value = chat.Id;
		param[1].Value = chat.AccessHash;
		param[2].Value = chat.IsForum ? 1 : 0;
		param[3].Value = (chat.Type == ChatType.Private ? chat.FirstName : chat.Title) ?? "";
		param[4].Value = chat.LastName ?? "";
		param[5].Value = chat.Username ?? "";
		param[6].Value = (int)chat.Type;
		//_cmd[SaveChat].ExecuteSave();
        cmd.ExecuteSave();
	}

	internal class CachedTable<T>(Func<long, T?> load, Action<T> save) where T : class
	{
		private readonly Dictionary<long, T> _cache = [];

		public T this[long key] { set => save(_cache[key] = value); }
		public bool TryGetValue(long key, [MaybeNullWhen(false)] out T value)
		{
			if (_cache.TryGetValue(key, out value)) return true;
			value = load(key);
			if (value == null) return false;
			_cache[key] = value;
			return true;
		}

		internal void ClearCache() => _cache.Clear();
		internal T? SearchCache(Predicate<T> predicate)
		{
			foreach (var chat in _cache.Values)
				if (predicate(chat))
					return chat;
			return null;
		}
	}
}

//TODO use bulk insert or selective update instead of full reupload of the tables
