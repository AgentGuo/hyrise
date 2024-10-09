import subprocess

def run_perf_monitoring(duration=10):
    # 定义需要监控的事件
    events = [
        'cache-references',       # 缓存访问次数
        'cache-misses',           # 缓存未命中次数
        'branch-instructions',    # 分支指令次数
        'branch-misses'           # 分支未命中次数
    ]

    # 拼接 perf 命令
    perf_command = ['perf', 'stat', '-e', ','.join(events), 'sleep', str(duration)]

    try:
        # 使用 subprocess 调用 perf 并捕获输出
        result = subprocess.run(perf_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # 输出通常在 stderr 中（perf 的统计信息在 stderr 中显示）
        perf_output = result.stderr

        # 解析结果
        cache_references = None
        cache_misses = None
        branch_instructions = None
        branch_misses = None

        for line in perf_output.splitlines():
            if 'cache-references' in line:
                cache_references = int(line.split()[0].replace(',', ''))
            elif 'cache-misses' in line:
                cache_misses = int(line.split()[0].replace(',', ''))
            elif 'branch-instructions' in line:
                branch_instructions = int(line.split()[0].replace(',', ''))
            elif 'branch-misses' in line:
                branch_misses = int(line.split()[0].replace(',', ''))

        # 计算缓存命中率和分支预测准确率
        if cache_references and cache_misses:
            cache_hit_rate = (cache_references - cache_misses) / cache_references * 100
        else:
            cache_hit_rate = None

        if branch_instructions and branch_misses:
            branch_prediction_accuracy = (branch_instructions - branch_misses) / branch_instructions * 100
        else:
            branch_prediction_accuracy = None

        return {
            'cache_references': cache_references,
            'cache_misses': cache_misses,
            'cache_hit_rate': cache_hit_rate,
            'branch_instructions': branch_instructions,
            'branch_misses': branch_misses,
            'branch_prediction_accuracy': branch_prediction_accuracy
        }

    except Exception as e:
        print(f"Error running perf: {e}")
        return None

if __name__ == '__main__':
    # 运行监控脚本，设置监控时长为 10 秒
    result = run_perf_monitoring(10)
    if result:
        print(f"Cache References: {result['cache_references']}")
        print(f"Cache Misses: {result['cache_misses']}")
        print(f"Cache Hit Rate: {result['cache_hit_rate']:.2f}%")
        print(f"Branch Instructions: {result['branch_instructions']}")
        print(f"Branch Misses: {result['branch_misses']}")
        print(f"Branch Prediction Accuracy: {result['branch_prediction_accuracy']:.2f}%")