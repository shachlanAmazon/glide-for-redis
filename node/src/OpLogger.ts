import percentile from "percentile";
import { stdev } from "stats-lite";

export class OpsLogger {
    private reads: [number, number][] = [];
    private writes: [number, number][] = [];
    private tic: [number, number] = [0, 0];

    private calculate_latency(
        latency_list: number[],
        percentile_point: number
    ) {
        const percentile_calculation = percentile(
            percentile_point,
            latency_list
        );
        const percentile_value = Array.isArray(percentile_calculation)
            ? percentile_calculation[0]
            : percentile_calculation;
        return Math.round(percentile_value * 100.0) / 100.0; // round to 2 decimal points
    }

    private latency_results(
        prefix: string,
        latencies: number[],
        postfix: string
    ): Record<string, number> {
        const result: Record<string, number> = {};
        const skip = latencies.length === 0;
        result[prefix + "_p50_" + postfix] = skip
            ? 0
            : this.calculate_latency(latencies, 50);
        result[prefix + "_p90_" + postfix] = skip
            ? 0
            : this.calculate_latency(latencies, 90);
        result[prefix + "_p99_" + postfix] = skip
            ? 0
            : this.calculate_latency(latencies, 99);
        result[prefix + "_average_" + postfix] = skip
            ? 0
            : latencies.reduce((a, b) => a + b, 0) / latencies.length;
        result[prefix + "_std_dev_" + postfix] = skip ? 0 : stdev(latencies);

        return result;
    }

    public getPrints(): Record<string, number> {
        const read_latencies = this.latency_results(
            "read",
            this.reads.map(([a, b]) => a),
            "latency"
        );
        const read_sizes = this.latency_results(
            "read",
            this.reads.map(([a, b]) => b),
            "size"
        );
        const write_sizes = this.latency_results(
            "write",
            this.writes.map(([a, b]) => b),
            "size"
        );
        const write_latencies = this.latency_results(
            "write",
            this.writes.map(([a, b]) => a),
            "latency"
        );
        return {
            ...read_latencies,
            ...read_sizes,
            ...write_sizes,
            ...write_latencies,
        };
    }

    private async act<T>(
        action: () => Promise<T>,
        sizeFunction: (arg: T) => number,
        list: [number, number][]
    ): Promise<T> {
        const tic = process.hrtime();
        const result = await action();
        const toc = process.hrtime(tic);
        const time = toc[0] * 1000 + toc[1] / 1000000;
        list.push([time, sizeFunction(result)]);
        return result;
    }

    public async write<T>(action: () => Promise<T>, size: number) {
        await this.act(action, () => size, this.writes);
    }

    public startWrite() {
        this.tic = process.hrtime();
    }

    public endWrite(size: number) {
        const toc = process.hrtime(this.tic);
        const time = toc[0] * 1000 + toc[1] / 1000000;
        this.writes.push([time, size]);
    }

    public async read<T>(
        action: () => Promise<T>,
        sizeFunction: (arg: T) => number
    ): Promise<T> {
        return this.act(action, sizeFunction, this.reads);
    }

    public endRead(size: number) {
        this.reads.push([0, size]);
    }
}
