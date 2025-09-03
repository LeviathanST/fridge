const std = @import("std");

const pg = @import("pg");
const util = @import("util.zig");

const Dialect = @import("connection.zig").Connection.Dialect;
const Error = @import("error.zig").Error;
const Statement = @import("statement.zig").Statement;
const Value = @import("value.zig").Value;

pub const PG = opaque {
    pub const Options = struct {
        conn_opts: pg.Conn.Opts,
        auth_opts: pg.Conn.AuthOpts,
    };
    var rows_affected: ?usize = null;

    pub fn open(alloc: std.mem.Allocator, opts: Options) !*PG {
        const pg_conn = alloc.create(pg.Conn) catch @panic("OOM");
        errdefer alloc.destroy(pg_conn);

        pg_conn.* = pg.Conn.openAndAuth(
            alloc,
            opts.conn_opts,
            opts.auth_opts,
        ) catch return error.ConnectionFailed;

        return @ptrCast(@alignCast(pg_conn));
    }

    pub fn dialect(_: *PG) Dialect {
        return .postgresql;
    }

    pub fn execAll(self: *PG, sql: []const u8) Error!void {
        const raw = (try check(?i64, self.ptr().exec(sql, .{}))).?;
        rows_affected = @intCast(raw);
    }

    pub fn prepare(self: *PG, sql: []const u8) Error!Statement {
        errdefer if (self.ptr().err) |e| util.log.err(
            "{s} {s}",
            .{ e.code, e.message },
        ) else util.log.err(
            "Uknown error",
            .{},
        );

        const alloc = self.ptr()._allocator;
        const transformed_sql = transformSatement(alloc, sql) catch @panic("OOM");
        defer alloc.free(transformed_sql);
        std.log.debug("transformed_sql \r\n//Start\r\n{s}\r\n//End", .{transformed_sql});
        var stmt: *pg.Stmt = alloc.create(pg.Stmt) catch @panic("OOM");
        errdefer alloc.destroy(stmt);

        stmt.* = try check(
            pg.Stmt,
            self.ptr().prepareOpts(transformed_sql, .{
                .allocator = alloc,
            }),
        );
        errdefer stmt.deinit();
        return util.upcast(@as(*Stmt, @ptrCast(stmt)), Statement);
    }

    /// TODO: should i make this work in query?
    fn transformSatement(alloc: std.mem.Allocator, raw: []const u8) ![]const u8 {
        var list = std.array_list.Aligned(u8, null).empty;
        defer list.deinit(alloc);
        var idx: usize = 1;

        for (raw) |c| {
            if (c == '?') {
                try list.writer(alloc).print("${d}", .{idx});
                idx += 1;
                continue;
            }
            try list.append(alloc, c);
        }
        return list.toOwnedSlice(alloc);
    }

    /// TODO: `pg.zig` just return rows affected from
    ///       conn.exec() or conn.execOpts().
    ///       What we need:
    ///       + How can we take **rows affected** variable when we execute
    ///         statements in `Stmt`?
    ///       + How to **execute statements** in `Stmt` since all of `conn.exec()`
    ///         and `conn.execOpts()` need sql string as an arg to run.
    pub fn rowsAffected(_: *PG) Error!usize {
        util.log.err(
            \\This function is not implemented in PG driver.
            \\If you want to ensure data is existed, you should
            \\use query.exists() and where() for condition.
            \\Example: 
            \\query.where("id", 1).exists()
        , .{});
        return rows_affected orelse 0;
    }

    /// This is not available in `pg.zig`, we
    /// should use `query.raw.returning("id")`.
    pub fn lastInsertRowId(self: *PG) Error!i64 {
        _ = self;
        return 0;
    }

    pub fn lastError(self: *PG) []const u8 {
        if (self.ptr().err) |err| {
            return err.message;
        }
        return "";
    }

    pub fn deinit(self: *PG) void {
        const alloc = self.ptr()._allocator;
        self.ptr().deinit();
        alloc.destroy(self.ptr());
    }

    inline fn ptr(self: *PG) *pg.Conn {
        return @ptrCast(@alignCast(self));
    }
};
const PGColumnType = enum {
    T_int2,
    T_int4,
    T_int8,
    T_float4,
    T_float8,
    T_varchar,
    T_char,
    T_bytea,
};
pub const Stmt = opaque {
    /// All values returned and managed by `pg.zig`
    var result: ?*pg.Result = null;
    /// If `result.next()` is not null, this will be modified
    /// as a current value
    var row: ?pg.Row = null;

    pub fn bind(self: *Stmt, index: usize, arg: Value) Error!void {
        _ = index; // ignore this since pg.zig handle index inside
        try check(void, switch (arg) {
            .null => self.ptr().bind(null),
            .int => self.ptr().bind(arg.int),
            .float => self.ptr().bind(arg.float),
            .blob => self.ptr().bind(arg.blob),
            .string => self.ptr().bind(arg.string),
        });
    }

    pub fn column(_: *Stmt, index: usize) Error!Value {
        const type_name = pg.types.oidToString(result.?._oids[index]);
        const value = result.?._values[index];

        if (std.mem.eql(u8, value.data, "")) {
            if (value.is_null) {
                return .null;
            } else return error.NotNullViolation;
        }

        inline for (std.meta.fields(PGColumnType)) |f| {
            if (std.mem.eql(u8, f.name, type_name)) {
                break;
            }
        } else return {
            util.log.err("{s} is not implemented yet in PG driver.", .{type_name});
            return error.DbError;
        };

        return parseValue(type_name, &(row orelse unreachable), index) catch |err| switch (err) {
            inline else => error.DbError,
        };
    }

    pub fn step(self: *Stmt) Error!bool {
        if (result) |r| {
            row = (r.next() catch return error.DbError) orelse
                return false;
            return true;
        } else {
            // NOTE: the result will be deinit in stmt.deinit()
            result = check(
                *pg.Result,
                self.ptr().execute(),
            ) catch return false;
            row = (result.?.next() catch return error.DbError) orelse
                return false;

            return true;
        }
    }

    pub fn reset(_: *Stmt) Error!void {}

    pub fn deinit(self: *Stmt) void {
        std.debug.assert(result != null);
        const alloc = self.ptr().conn._allocator;

        result.?.deinit();
        result.?.drain() catch |err| {
            std.log.err("Failed to drain PG messages: {}", .{err});
            self.ptr().conn._state = .fail;
            return;
        };
        result = null;
        alloc.destroy(self.ptr());
    }

    pub fn ptr(self: *Stmt) *pg.Stmt {
        return @ptrCast(@alignCast(self));
    }
};

fn parseValue(pg_type_name: []const u8, row: *const pg.Row, col: usize) !Value {
    return switch (std.meta.stringToEnum(PGColumnType, pg_type_name).?) {
        .T_int2 => Value{ .int = @intCast(row.get(i16, col)) },
        .T_int4 => Value{ .int = @intCast(row.get(i32, col)) },
        .T_int8 => Value{ .int = @intCast(row.get(i64, col)) },
        .T_float4, .T_float8 => Value{ .float = row.get(f64, col) },
        .T_varchar, .T_char => Value{ .string = row.get([]const u8, col) },
        else => .null,
    };
}

fn check(comptime Returned: type, data_or_err: anytype) !Returned {
    if (data_or_err) |data| {
        return data;
    } else |err| {
        const typeOf = @TypeOf(err);
        return switch (@typeInfo(typeOf)) {
            .error_set => switch (err) {
                error.NoSpaceLeft => error.OutOfMemory,
                else => Error.DbError,
            },
            else => unreachable,
        };
    }
}

const testing = std.testing;
test "unkown host name" {
    const alloc = testing.allocator;
    try testing.expectError(
        error.UnknownHostName,
        @import("session.zig").Session.open(PG, alloc, .{
            .conn_opts = .{
                .host = "unknown_host",
                .port = 5432,
            },
            .auth_opts = .{},
        }),
    );
}
