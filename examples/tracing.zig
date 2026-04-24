const std = @import("std");
const zart = @import("zart");

const WorkerMsg = union(enum) {
    run,
};

const Worker = struct {
    pub const Msg = WorkerMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        _ = try ctx.recv();
    }
};

const Recorder = struct {
    count: usize = 0,

    fn tracer(self: *@This()) zart.Tracer {
        return .{
            .context = self,
            .event_fn = record,
        };
    }

    fn record(context: ?*anyopaque, _: zart.TraceEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.count += 1;
    }
};

pub fn main(_: std.process.Init) !void {
    var recorder: Recorder = .{};
    var rt = try zart.Runtime.init(std.heap.page_allocator, .{
        .tracer = recorder.tracer(),
    });
    defer rt.deinit();

    const worker = try rt.spawn(Worker{});
    try worker.send(.run);
    try rt.run();

    std.debug.print("trace events = {d}\n", .{recorder.count});
}
