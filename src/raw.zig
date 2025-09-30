//! TODO: QueryBuilder for each driver?
const std = @import("std");
const Session = @import("session.zig").Session;
const Dialect = @import("connection.zig").Connection.Dialect;
const Value = @import("value.zig").Value;
const Statement = @import("statement.zig").Statement;
const SqlBuf = @import("sql.zig").SqlBuf;
const util = @import("util.zig");

pub fn Part(comptime dialect: Dialect) type {
    return struct {
        prev: ?*const Part(dialect) = null,
        kind: Kind,
        sql: []const u8 = "",
        args: Args(dialect) = .none,

        pub const Kind = enum { raw, cols, table, SELECT, INSERT, UPDATE, DELETE, JOIN, @"LEFT JOIN", WHERE, AND, OR, @"GROUP BY", HAVING, @"ORDER BY", VALUES, SET, @"ON CONFLICT", RETURNING };

        pub fn toSql(self: Part(dialect), buf: *SqlBuf) !void {
            if (self.prev) |p| try buf.append(p);

            if (self.kind != .raw and self.kind != .cols and self.kind != .table) {
                const comma = switch (self.kind) {
                    .SELECT, .@"GROUP BY", .@"ORDER BY", .SET => self.prev != null and self.prev.?.kind == self.kind,
                    else => false,
                };

                try buf.append(if (comma) ", " else switch (self.kind) {
                    .SELECT => "SELECT ",
                    inline .INSERT, .UPDATE, .DELETE => |t| @tagName(t),
                    inline else => |t| " " ++ @tagName(t) ++ " ",
                });
            }

            try buf.append(self.sql);
        }

        fn bind(self: Part(dialect), stmt: anytype, i: *usize) !void {
            if (self.prev) |p| try p.bind(stmt, i);

            try self.args.bind(stmt, i);
        }
    };
}

pub fn Query(comptime dialect: Dialect) type {
    const DialectSession = Session(dialect);
    const DialectPart = Part(dialect);
    return struct {
        const Self = @This();

        db: *DialectSession,
        parts: struct {
            head: ?*const DialectPart = null, //   <raw>, SELECT, INSERT, UPDATE, DELETE
            tables: ?*const DialectPart = null, // JOIN, LEFT JOIN, cols, VALUES, SET
            where: ?*const DialectPart = null, //  WHERE, AND, OR
            tail: ?*const DialectPart = null, //   everything else
        } = .{},

        pub fn init(db: *DialectSession) Self {
            return .{ .db = db };
        }

        pub fn raw(db: *DialectSession, sql: []const u8, args: anytype) Self {
            return init(db).append(.raw, sql, .from(args, db));
        }

        pub fn table(self: Self, sql: []const u8) Self {
            const part = self.db.arena.create(DialectPart) catch @panic("OOM");
            part.* = .{ .kind = .table, .sql = sql };
            return self.replace(part);
        }

        pub fn insert(self: Self) Self {
            return self.replace(comptime &.{ .kind = .INSERT });
        }

        pub const into = table;

        pub fn cols(self: Self, sql: []const u8) Self {
            return self.append(.cols, sql, .none);
        }

        pub fn values(self: Self, sql: []const u8, args: anytype) Self {
            return self.append(.VALUES, sql, .fromFields(args, self.db));
        }

        pub fn onConflict(self: Self, sql: []const u8, args: anytype) Self {
            return self.append(.@"ON CONFLICT", sql, .from(args, self.db));
        }

        pub fn update(self: Self) Self {
            return self.replace(comptime &.{ .kind = .UPDATE });
        }

        pub fn set(self: Self, sql: []const u8, args: anytype) Self {
            return self.append(.SET, sql, .from(args, self.db));
        }

        pub fn setAll(self: Self, data: anytype) Self {
            if (comptime std.meta.fields(@TypeOf(data)).len == 0) {
                return self;
            }

            return self.append(.SET, util.setters(@TypeOf(data)), .fromFields(data, self.db));
        }

        pub fn delete(self: Self) Self {
            return self.replace(comptime &.{ .kind = .DELETE });
        }

        pub fn select(self: Self, sql: []const u8) Self {
            return self.selectRaw(sql, {});
        }

        pub fn selectRaw(self: Self, sql: []const u8, args: anytype) Self {
            const part = self.db.arena.create(Part(dialect)) catch @panic("OOM");
            part.* = .{ .kind = .SELECT, .sql = sql, .args = .from(args, self.db) };
            return self.replace(part);
        }

        pub const from = table;

        pub fn join(self: Self, sql: []const u8) Self {
            return self.append(.JOIN, sql, .none);
        }

        pub fn leftJoin(self: Self, sql: []const u8) Self {
            return self.append(.@"LEFT JOIN", sql, .none);
        }

        pub fn where(self: Self, sql: []const u8, args: anytype) Self {
            return self.append(if (self.parts.where == null) .WHERE else .AND, sql, .from(args, self.db));
        }

        pub fn ifWhere(self: Self, cond: bool, sql: []const u8, args: anytype) Self {
            return if (cond) self.where(sql, args) else self;
        }

        pub fn maybeWhere(self: Self, comptime sql: []const u8, arg: anytype) Self {
            return if (arg) |v| self.where(sql, v) else self;
        }

        pub fn orWhere(self: Self, sql: []const u8, args: anytype) Self {
            return self.append(if (self.parts.where == null) .WHERE else .OR, sql, .from(args, self.db));
        }

        pub fn orIfWhere(self: Self, cond: bool, sql: []const u8, args: anytype) Self {
            return if (cond) self.orWhere(sql, args) else self;
        }

        pub fn orMaybeWhere(self: Self, comptime sql: []const u8, arg: anytype) Self {
            return if (arg) |v| self.orWhere(sql, v) else self;
        }

        pub fn groupBy(self: Self, sql: []const u8) Self {
            return self.append(.@"GROUP BY", sql, .none);
        }

        pub fn having(self: Self, sql: []const u8, args: anytype) Self {
            return self.append(.HAVING, sql, .from(args, self.db));
        }

        pub fn orderBy(self: Self, sql: []const u8) Self {
            return self.append(.@"ORDER BY", sql, .none);
        }

        pub fn limit(self: Self, n: i32) Self {
            return self.append(.raw, " LIMIT ?", .{ .one = .{ .int = @intCast(n) } });
        }

        pub fn offset(self: Self, i: i32) Self {
            return self.append(.raw, " OFFSET ?", .{ .one = .{ .int = @intCast(i) } });
        }

        pub fn returning(self: Self, sql: []const u8) Self {
            return self.append(.RETURNING, sql, .none);
        }

        pub fn exec(self: Self) !void {
            var stmt = try self.prepare();
            defer stmt.deinit();

            try stmt.exec();
        }

        pub fn get(self: Self, comptime T: type) !?T {
            var stmt = try self.prepare();
            defer stmt.deinit();

            return if (try stmt.next(?T, self.db.arena)) |v| v else null;
        }

        pub fn exists(self: Self) !bool {
            return try self.select("1").get(bool) orelse false;
        }

        pub fn count(self: Self, comptime col: []const u8) !u64 {
            return (try self.select("COUNT(" ++ col ++ ")").get(u64)).?;
        }

        pub fn pluck(self: Self, comptime R: type) ![]const R {
            var stmt = try self.prepare();
            defer stmt.deinit();

            var res = std.array_list.Managed(R).init(self.db.arena);
            errdefer res.deinit();

            while (try stmt.next(struct { R }, self.db.arena)) |row| {
                try res.append(row[0]);
            }

            return res.toOwnedSlice();
        }

        pub fn fetchOne(self: Self, comptime R: type) !?R {
            var stmt = try self.prepare();
            defer stmt.deinit();

            return stmt.next(R, self.db.arena);
        }

        pub fn fetchAll(self: Self, comptime R: type) ![]const R {
            var stmt = try self.prepare();
            defer stmt.deinit();

            var res = std.array_list.Managed(R).init(self.db.arena);
            errdefer res.deinit();

            while (try stmt.next(R, self.db.arena)) |row| {
                try res.append(row);
            }

            return res.toOwnedSlice();
        }

        pub fn toSql(self: Self, buf: *SqlBuf) !void {
            if (self.parts.head) |h| try buf.append(h);

            if (self.parts.tables) |t| {
                std.debug.assert(buf.buf.items.len > 1);
                try buf.append(switch (std.ascii.toLower(buf.buf.items[1])) {
                    'e' => " FROM ", // SeLECT, DeLETE
                    'n' => " INTO ", // InSERT
                    else => " ",
                });

                try buf.append(t);
            }

            if (self.parts.where) |w| try buf.append(w);
            if (self.parts.tail) |t| try buf.append(t);
        }

        pub fn prepare(self: Self) !Statement {
            var buf = try SqlBuf.init(self.db.arena);
            try buf.append(self);

            var stmt = try self.db.conn.prepare(buf.buf.items);
            errdefer stmt.deinit();

            var i: usize = 0;
            if (self.parts.head) |p| try p.bind(&stmt, &i);
            if (self.parts.tables) |t| try t.bind(&stmt, &i);
            if (self.parts.where) |w| try w.bind(&stmt, &i);
            if (self.parts.tail) |t| try t.bind(&stmt, &i);

            return stmt;
        }

        pub fn append(self: Self, kind: DialectPart.Kind, sql: []const u8, args: Args(dialect)) Self {
            const part = self.db.arena.create(Part(dialect)) catch @panic("OOM");
            part.* = .{ .prev = self.slot(kind).*, .kind = kind, .sql = sql, .args = args };
            return self.replace(part);
        }

        fn replace(self: Self, part: *const DialectPart) Self {
            var copy = self;
            copy.slot(part.kind).* = part;
            return copy;
        }

        fn slot(self: anytype, kind: DialectPart.Kind) switch (@TypeOf(self)) {
            *const Self => *const ?*const DialectPart,
            *Self => *?*const DialectPart,
            else => unreachable,
        } {
            return switch (kind) {
                .raw => if (self.parts.head == null) &self.parts.head else &self.parts.tail,
                .SELECT, .INSERT, .UPDATE, .DELETE => &self.parts.head,
                .table, .cols, .VALUES, .SET, .JOIN, .@"LEFT JOIN" => &self.parts.tables,
                .WHERE, .AND, .OR => &self.parts.where,
                else => &self.parts.tail,
            };
        }
    };
}

fn Args(comptime dialect: Dialect) type {
    const DialectSession = Session(dialect);
    return union(enum) {
        const DialectArg = @This();

        none,
        one: Value,
        many: []const Value,

        fn from(args: anytype, db: *DialectSession) DialectArg {
            if (comptime @TypeOf(args) == Args(dialect)) return args;
            if (comptime @TypeOf(args) == void) return .none;
            if (comptime util.isTuple(@TypeOf(args))) return fromFields(args, db);

            return .{ .one = Value.from(args, db.arena) catch @panic("OOM") };
        }

        fn fromFields(args: anytype, db: *DialectSession) DialectArg {
            const fields = std.meta.fields(@TypeOf(args));
            const res = db.arena.alloc(Value, fields.len) catch @panic("OOM");
            inline for (fields, 0..) |f, i| res[i] = Value.from(@field(args, f.name), db.arena) catch @panic("OOM");

            return .{ .many = res };
        }

        fn bind(self: DialectArg, stmt: anytype, i: *usize) !void {
            switch (self) {
                .none => {},
                .one => |arg| try arg.bind(stmt, i),
                .many => |args| for (args) |arg| try arg.bind(stmt, i),
            }
        }
    };
}

const expectSql = @import("testing.zig").expectSql;
const fakeDb = @import("testing.zig").fakeDb;

test "select" {
    var db = try fakeDb();
    defer db.deinit();
    const select1 = Query(.other).init(&db).select("1");

    try expectSql(select1, "SELECT 1");
    try expectSql(select1.select("2"), "SELECT 2");
    try expectSql(select1.selectRaw("?", 1), "SELECT ?");
}

test "insert" {
    var db = try fakeDb();
    defer db.deinit();
    const insert = Query(.other).init(&db).insert();

    try expectSql(insert, "INSERT");
    try expectSql(insert.into("Person"), "INSERT INTO Person");
    try expectSql(insert.into("Person").returning("id"), "INSERT INTO Person RETURNING id");
    try expectSql(insert.into("Person").cols("(name, age)"), "INSERT INTO Person(name, age)");
    try expectSql(insert.into("Person").cols("(name, age)").values("(?, ?)", .{ "Alice", 18 }), "INSERT INTO Person(name, age) VALUES (?, ?)");
}

test "update" {
    var db = try fakeDb();
    defer db.deinit();
    const update = Query(.other).init(&db).update();

    try expectSql(update, "UPDATE");
    try expectSql(update.table("Person"), "UPDATE Person");
    try expectSql(update.table("Person").set("age = ?", 18), "UPDATE Person SET age = ?");
}

test "delete" {
    var db = try fakeDb();
    defer db.deinit();
    const delete = Query(.other).init(&db).delete();

    try expectSql(delete, "DELETE");
    try expectSql(delete.from("Person"), "DELETE FROM Person");
    try expectSql(delete.from("Person").where("age < ?", 18), "DELETE FROM Person WHERE age < ?");
}

test "raw" {
    var db = try fakeDb();
    defer db.deinit();
    const raw = db.raw("SELECT DISTINCT name", {});

    try expectSql(raw, "SELECT DISTINCT name");
    try expectSql(raw.from("Person"), "SELECT DISTINCT name FROM Person");
    try expectSql(raw.from("Person").where("age > ?", 18), "SELECT DISTINCT name FROM Person WHERE age > ?");
}
