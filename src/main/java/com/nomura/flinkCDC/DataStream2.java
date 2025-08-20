package com.nomura.flinkCDC;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;

public class DataStream2 {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // =======================
        // 2. 配置 Checkpoint
        // =======================

        // 定义 Checkpoint 存储根目录
        File checkpointRootDir = new File("tmp/checkpoint");
        if (!checkpointRootDir.exists()) {
            checkpointRootDir.mkdirs();
            System.out.println("✅ 创建 Checkpoint 目录: " + checkpointRootDir.getAbsolutePath());
        }

        String checkpointStoragePath = "file:///" + checkpointRootDir.getAbsolutePath().replace("\\", "/");

        // 设置 Checkpoint 基础配置
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointStorage(checkpointStoragePath);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // ✅ 重要：保留 Checkpoint，即使任务取消也不删除（调试必备）
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointingConfiguration.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );

        // =======================
        // 3. 决定从哪个 Checkpoint 恢复
        // =======================

        String restorePath = null;

        // 优先：使用命令行参数指定的路径
        if (args.length > 0) {
            restorePath = args[0];
            System.out.println("🚀 正在从指定 Checkpoint 恢复: " + restorePath);
        }
        // 其次：自动查找最新的已完成 Checkpoint
        else {
            Optional<String> latestCheckpoint = findLatestCompletedCheckpoint(checkpointRootDir);
            if (latestCheckpoint.isPresent()) {
                restorePath = latestCheckpoint.get();
                System.out.println("🔄 自动恢复最新 Checkpoint: " + restorePath);
            } else {
                System.out.println("🆕 未找到已有 Checkpoint，将启动全新任务");
            }
        }

        // 如果找到了恢复路径，设置为 StateBackend
        if (restorePath != null) {
            env.setStateBackend(new FsStateBackend(restorePath));
        } else {
            // 否则使用默认存储路径
            env.setStateBackend(new FsStateBackend(checkpointStoragePath));
        }

        // =======================
        // 4. 添加你的 CDC Source
        // =======================
        // MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
        //     ... 你的配置
        //     .build();

        // env.addSource(mysqlSource).print();

        // =======================
        // 5. 执行任务
        // =======================
        env.execute("Flink CDC Job - With Checkpoint Recovery");
    }

    /**
     * 查找 Checkpoint 目录下最新的已完成 Checkpoint（chk-x）
     */
    private static Optional<String> findLatestCompletedCheckpoint(File checkpointRootDir) {
        try {
            Path rootPath = Paths.get(checkpointRootDir.getAbsolutePath());

            // 查找所有 chk-* 目录，并按名称排序（数字越大越新）
            return Files.list(rootPath)
                    .filter(Files::isDirectory)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.startsWith("chk-"))
                    .max(Comparator.comparingInt(s -> {
                        try {
                            return Integer.parseInt(s.substring(4));
                        } catch (NumberFormatException e) {
                            return 0;
                        }
                    }))
                    .map(chkDirName -> rootPath.resolve(chkDirName).toUri().toString());
        } catch (Exception e) {
            System.err.println("⚠️ 无法查找 Checkpoint: " + e.getMessage());
            return Optional.empty();
        }
    }
}
