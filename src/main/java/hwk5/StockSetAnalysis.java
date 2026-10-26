package hwk5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 作业5：求每个指数下 A 与 B 的成分股集合关系。
 *
 * <p>任务1：对每个指数编号和股票代码，判断其仅出现在 A、仅出现在 B，还是同时出现在 A 和 B 中。
 * 任务2：对任务1的结果进行二次统计，输出每个指数的交集、A 独有、B 独有以及并集数量。
 *
 * <p>用法：{@code hadoop jar hwk5.jar <input_dir> <output_task1> <output_task2>}
 */
public class StockSetAnalysis {

    /** 任务1 Mapper：读入 A.txt / B.txt 的每一行，按来源打上标签。 */
    public static class SetRelationMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            // 输入行：指数编号<tab或空格>股票代码
            String[] fields = line.split("\\s+");
            if (fields.length < 2) {
                return;
            }
            String index = fields[0];
            String stock = fields[1];

            // 通过文件名判断记录来自 A.txt 还是 B.txt
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            String source = fileName.startsWith("A") ? "A" : "B";

            // 以 “指数编号\t股票代码” 作为 key，value 为来源标签 A/B
            outKey.set(index + "\t" + stock);
            outValue.set(source);
            context.write(outKey, outValue);
        }
    }

    /** 任务1 Reducer：根据同一 (指数, 股票) 下出现过的来源标签判断集合关系。 */
    public static class SetRelationReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean inA = false;
            boolean inB = false;
            for (Text v : values) {
                if ("A".equals(v.toString())) {
                    inA = true;
                } else if ("B".equals(v.toString())) {
                    inB = true;
                }
            }
            String relation;
            if (inA && inB) {
                relation = "BOTH";
            } else if (inA) {
                relation = "A_ONLY";
            } else {
                relation = "B_ONLY";
            }
            // 输出：指数编号 \t 股票代码 \t 关系标记（key 中已含指数编号和股票代码）
            outValue.set(relation);
            context.write(key, outValue);
        }
    }

    /** 任务2 Mapper：读入任务1输出 “指数编号\t股票代码\t关系”，仅保留指数编号与关系。 */
    public static class SetStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] fields = line.split("\\t");
            if (fields.length < 3) {
                return;
            }
            outKey.set(fields[0]);        // 指数编号
            outValue.set(fields[2]);      // 关系标记 BOTH / A_ONLY / B_ONLY
            context.write(outKey, outValue);
        }
    }

    /** 任务2 Reducer：统计每个指数下各关系标记的数量。 */
    public static class SetStatsReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int both = 0, aOnly = 0, bOnly = 0;
            for (Text v : values) {
                String r = v.toString();
                if ("BOTH".equals(r)) {
                    both++;
                } else if ("A_ONLY".equals(r)) {
                    aOnly++;
                } else if ("B_ONLY".equals(r)) {
                    bOnly++;
                }
            }
            int union = both + aOnly + bOnly;
            // 输出：指数编号 \t 交集数量 \t A独有数量 \t B独有数量 \t 并集数量
            outValue.set(both + "\t" + aOnly + "\t" + bOnly + "\t" + union);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: StockSetAnalysis <input_dir> <output_task1> <output_task2>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path outputTask1 = new Path(args[1]);
        Path outputTask2 = new Path(args[2]);

        Configuration conf = new Configuration();

        // ---------- 任务1 ----------
        Job job1 = Job.getInstance(conf, "task1: set relation of A and B");
        job1.setJarByClass(StockSetAnalysis.class);
        job1.setMapperClass(SetRelationMapper.class);
        job1.setReducerClass(SetRelationReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, outputTask1);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ---------- 任务2 ----------
        Job job2 = Job.getInstance(conf, "task2: intersection/union statistics per index");
        job2.setJarByClass(StockSetAnalysis.class);
        job2.setMapperClass(SetStatsMapper.class);
        job2.setReducerClass(SetStatsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, outputTask1);
        FileOutputFormat.setOutputPath(job2, outputTask2);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
