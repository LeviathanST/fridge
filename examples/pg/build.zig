const std = @import("std");

pub fn build(b: *std.Build) void {
    const t = b.standardTargetOptions(.{});
    const o = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "pg_examples",
        .root_module = b.createModule(.{
            .root_source_file = b.path("main.zig"),
            .optimize = o,
            .target = t,
        }),
    });
    b.installArtifact(exe);

    const fridge_mod = b.dependency("fridge", .{}).module("fridge");
    exe.root_module.addImport("fridge", fridge_mod);

    const run_exe = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run the PG example");
    run_step.dependOn(&run_exe.step);
}
