import os
import time
import logging
import schedule
import subprocess
from functools import partial

# 初始化logging
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 初始化task_dict
task_dict = {}

def run_python_script(file_path):
    try:
        log_file = os.path.splitext(file_path)[0] + '.log'
        with open(log_file, 'a') as f:
            subprocess.Popen(['python', file_path], stdout=f, stderr=f)
            logging.info(f"Started running script {file_path}")
    except Exception as e:
        logging.error(f"Failed to run script {file_path}: {e}")

def get_python_files(directory):
    python_files = set()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                python_files.add(os.path.join(root, file))
    return python_files

def update_file_sets(current_files, previous_files):
    added_files = current_files - previous_files
    removed_files = previous_files - current_files
    previous_files.clear()
    previous_files.update(current_files)
    return added_files, removed_files

def is_valid_time(task_type, specific_time):
    try:
        if task_type == 'day':
            time.strptime(specific_time, '%H:%M')
        elif task_type == 'hour':
            time.strptime(specific_time, '%M')
        elif task_type == 'minute':
            time.strptime(specific_time, '%S')
        elif task_type == 'week':
            day_of_week, time_of_day = specific_time.split()
            time.strptime(time_of_day, '%H:%M')
            if day_of_week.capitalize() not in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
                return False
        else:
            return False
    except ValueError:
        return False
    return True

def create_scheduled_tasks(added_files, task_dict):
    for file in added_files:
        # 获取文件上两级的目录名称
        parent_dir = os.path.dirname(os.path.dirname(file))
        task_type = os.path.basename(parent_dir)
        specific_time = os.path.basename(os.path.dirname(file)).replace('_', ':')

        # 检查 specific_time 的合法性
        if not is_valid_time(task_type, specific_time):
            logging.error(f"无效的时间格式: {specific_time} 对于任务类型: {task_type}")
            continue

        # 根据获取到的定时任务信息创建定时执行指定文件的定时任务
        task = None
        if task_type == 'day':
            task = schedule.every().day.at(specific_time).do(partial(run_python_script, file))
            logging.info(f"创建每日任务: {file} 在 {specific_time}")
        elif task_type == 'hour':
            task = schedule.every().hour.at(f':{specific_time}').do(partial(run_python_script, file))
            logging.info(f"创建每小时任务: {file} 在 :{specific_time}")
        elif task_type == 'minute':
            task = schedule.every().minute.at(f':{specific_time}').do(partial(run_python_script, file))
            logging.info(f"创建每分钟任务: {file} 在 :{specific_time}")
        elif task_type == 'week':
            day_of_week, time_of_day = specific_time.split()
            day_of_week = day_of_week.capitalize()
            if hasattr(schedule.every(), day_of_week):
                task = getattr(schedule.every(), day_of_week).at(time_of_day).do(partial(run_python_script, file))
                logging.info(f"创建每周任务: {file} 在 {time_of_day} 的 {day_of_week}")
        else:
            logging.error(f"未知的定时任务类型: {task_type}")
            continue
        # 将任务实例记录在字典中
        if task:
            task_dict[file] = task

def remove_scheduled_tasks(removed_files, task_dict):
    for file in removed_files:
        if file in task_dict:
            schedule.cancel_job(task_dict[file])
            del task_dict[file]

def check_directory_changes(cron_dir, previous_files=set()):
    global task_dict
    # Get the current state of the directory and its subdirectories
    current_files = get_python_files(cron_dir)

    # Find added and removed files
    added_files, removed_files = update_file_sets(current_files, previous_files)

    if added_files:
        logging.info("新增文件: %s", list(added_files))
        create_scheduled_tasks(added_files, task_dict)

    if removed_files:
        logging.info("删除的文件: %s", list(removed_files))
        remove_scheduled_tasks(removed_files, task_dict)

    # Update the previous state to the current one
    previous_files = current_files

def check_scheduled_jobs():
    jobs = schedule.get_jobs()
    logging.info("当前调度的任务列表:")
    for job in jobs:
        logging.info(f"Job: {job}, Last Run Time: {job.last_run}, Next Run Time: {job.next_run}")

if __name__ == '__main__':
    # 定期每秒检查目录变化
    cron_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cron')
    cron_files = set()
    schedule.every(1).seconds.do(check_directory_changes, cron_dir, cron_files)

    # 定期报告一次各任务状态
    schedule.every(1).hour.do(check_scheduled_jobs)

    # 运行调度
    while True:
        schedule.run_pending()
        time.sleep(1)
