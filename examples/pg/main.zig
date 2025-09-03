//! NOTE: This example is just a PG client, so you have
//! to run Postgresql server (via docker, psql, ...)
//! with credentials:
//! + host: localhost
//! + port: 5432
//! + database: db
//! + username: root
//! + password: root
//!
//! NOTE: If you run this program in DEBUG mode,
//! you can see queries debug.
const std = @import("std");
const fr = @import("fridge");

const User = struct {
    id: i64,
    username: []const u8,
    password: []const u8,

    pub const sql_table_name =
        \\"users"
    ;
};

pub fn main() !void {
    var debug_alloc = std.heap.DebugAllocator(.{}).init;
    defer if (debug_alloc.deinit() == .leak) std.log.warn(
        "Leak memory is detected in PG example",
        .{},
    );

    const alloc = debug_alloc.allocator();
    var pool = try fr.Pool(fr.PG).init(debug_alloc.allocator(), .{}, .{
        .conn_opts = .{
            .host = "localhost",
            .port = 5432,
        },
        .auth_opts = .{
            .database = "db",
            .username = "root",
            .password = "root",
        },
    });
    defer pool.deinit();

    var s = try pool.getSession(alloc);
    defer s.deinit();
    errdefer {
        std.log.err("{s}", .{s.conn.lastError()});
    }

    { // Create new `users` table if it not exists
        const schema = s.schema();
        try schema.createTable(
            "users",
            true,
        ).column(
            "id",
            .int,
            .{ .primary_key = true },
        ).column(
            "username",
            .text,
            .{ .unique = true },
        ).column(
            "password",
            .text,
            .{},
        ).exec();
    }

    // Remove a user where `username` = "hoho"
    try s.query(User).where(
        "username",
        "hoho",
    ).delete().exec();

    // Create a user with `username` = "hoho"
    try s.query(User).insert(.{
        .id = 1, // TODO: SERIAL type
        .username = "hoho",
        .password = "hoho",
    }).exec();

    // Find a user where `username` = "hoho"
    const maybe_user = try s.query(User).findBy("username", "hoho");
    if (maybe_user) |user| {
        std.log.info("User: {s}", .{user.username});
    } else {
        std.log.info("Not found", .{});
    }
}
