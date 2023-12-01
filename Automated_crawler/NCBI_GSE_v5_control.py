# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 16:07:05 2023
xxxxxx
@author: maxizheng
"""
#导入selenium包
import os
os.chdir("C:\\Users\\maxizheng\\Desktop\\2023_09\\NCBI_GSE")
#os.chdir("D:\\Workspace_xzma\\Python_pycharm")
from selenium import webdriver
#import src
#from src.parse import *
import concurrent.futures
import yaml
import threading
from bs4 import BeautifulSoup
import time
import requests
import random
#%%
def distribute_yaml(yaml_data):
    global dilivery_once, storage_num, mission_num, dilivery_start, dilivery_end, keep, webdrive, parse, parse_type, download_html, TIMEOUT
    parameter_list = ['dilivery_once', 'storage_num', 'mission_num', 'dilivery_start', 'dilivery_end', 'keep', 'webdrive', 'parse', 'parse_type', 'download_html', 'TIMEOUT']
    for para in parameter_list:
        globals()[para] = yaml_data['Configure'][para]
        #dilivery_once = yaml_data['Configure']['dilivery_once']
        print(f"global var {para} :{eval(para)}")
def get_url_list(yaml_data):
    #mode = yaml_data['URL']['mode']
    global mode, base, ranger
    parameter_list = ['mode', 'base', 'ranger']
    for para in parameter_list:
        globals()[para] = yaml_data['URL'][para]
        #exec(f"{para}='{yaml_data['URL'][para]}'")
    if mode == 'loop_range':
        url_list = loop_range(base, ranger)
    elif mode == "loop_range_file":
        url_list = loop_range_file(base, ranger)
    else:
        exit("mode cant be deal now!")
    return url_list
def loop_range(base, ranger):
    base_list = base.split("%%%%%%")
    range_list = ["object_"+str(i) for i in range(ranger)]
    url_list = [str(i).join(base_list) for i in range(ranger)]
    return range_list, url_list
def loop_range_file(base, ranger):
    base_list = base.split("%%%%%%")
    with open(ranger, 'rt') as FF:
        FF_list = FF.readlines()
        range_list = ["object_"+i.strip() for i in FF_list]
        url_list = [i.strip().join(base_list) for i in FF_list]
    return range_list ,url_list
def Count_initialization():
    global loop_num
    global write_time
    global RF
    global dilivery_time
    global dilivery_end
    global result_dir
    global web_list
    global options_list
    loop_num = 0; write_time = 0
    dilivery_time = dilivery_start
    if dilivery_time != 0:
        shang, yu = divmod((dilivery_time * dilivery_once + 1), storage_num)
        loop_num = dilivery_time * dilivery_once
        write_time = dilivery_time * dilivery_once
        if yu != 1:
            RF = open(result_dir+str(shang)+".list", 'at')
            #RF.write("add")
    if dilivery_end == "none" or dilivery_end == "":
        dilivery_end = float('inf')
    if not os.path.exists(result_dir):
        os.mkdir(result_dir)
    web_list = [webdriver.Firefox, webdriver.Chrome]
    options_Firefox = webdriver.FirefoxOptions()
    options_Chrome = webdriver.ChromeOptions()
    #options.set_headless(True)
    options_Firefox.add_argument("--headless") #设置火狐为headless无界面模式
    options_Chrome.add_argument("--headless")
    #options.add_argument("--disable-gpu")
    options_list = [options_Firefox, options_Chrome]
def pycharm_task(ranger, url, keep, webdriver, parse, download_html): #此处的webdriver 成了线程间共享的, 需要优化掉
    if isinstance(url, list): #采用递归的方法，非列表直接运行，列表的话会进入该函数的非列表情况
        extract_result_list = []
        for u_index in range(len(url)):
            r = ranger[u_index]
            u = url[u_index]
            re = pycharm_task(r, u, keep, webdriver, parse, download_html)
            extract_result_list.append(re)
        extract_result = "\n".join(extract_result_list)
    else:
        if webdrive:
            #if keep: driver = webdriver.Firefox(options=options) pages = get_html_Keep(driver, url) #else:
            pages = get_html_restart(url)
        else:
            pass #request
        if parse:
            #extract_result = parse_html(pages, genecards_parse)
            extract_result = parse_html(pages, eval(parse_type))
        else:
            extract_result = f"{url} dont parse"
        if download_html:
            with open(result_dir+ranger+".html", 'wt', encoding = "utf-8") as FF:
                print(f"{url} is downloading")
                FF.write(pages)
                random_TIMEOUT = random.randint(TIMEOUT-4,TIMEOUT+4)
                time.sleep(random_TIMEOUT) #增加程序等待时间减少IO压力
                #time.sleep(TIMEOUT)
    return extract_result
def write(result, storage_num):
    #全局变量不可以放在参数中， 因为局部变量和全局变量会冲突
    global write_time
    global RF
    write_time += 1
    #print(f"---check write_time {write_time}")
    shang, yu = divmod(write_time, storage_num)
    if yu == 1:
        RF_name = result_dir+"Result_Part"+str(shang)+".list"
        RF = open(RF_name, 'wt')
        RF.write(f"{result}\n")
        RF.flush()
    elif yu == 0:
        RF.write(f"{result}\n")
        time.sleep(10) #waite
        #input_with_timeout("解析开关文件", 10)
        RF.close()
    else:
        RF.write(f"{result}\n")
        RF.flush()
def parse_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        yaml_data = yaml.safe_load(file)
        return yaml_data
def alarm_handler():
    raise TimeoutError("Input timeout")
def alarm_executor():
    global executor
    for future in futures:
        future.cancel()
    executor.shutdown(wait=False)
    print("executor shutdown and restart!!!")
    executor = concurrent.futures.ThreadPoolExecutor()
    raise TimeoutError("Loop timeout")
def input_with_timeout(prompt, timeout, yaml_file):
    # 设置定时器
    timer = threading.Timer(timeout, alarm_handler)
    Switch = False
    print("---Switch Judgment Start---")
    try:  
        timer.start()
        # 获取用户输入#user_input = input(prompt) #spyder 无法使用input
        user_input = open(yaml_file,'rt').readlines()[-1].split(":")[-1].strip()
        # 获取输入后取消定时器
        timer.cancel()
        #return user_input
        if user_input != "on" and user_input != "On":
            raise Exception("turn off")
    except TimeoutError:
        print("Recognize occur timeout error!")
    except Exception:
        print("Recognize that the switch is turn off!")
    else:
        print("Recognize that the switch is on!")
        Switch = True
    finally:
        print("---Switch Judgment complete---")
    return Switch
    #user_input = input_with_timeout("请输入：", 5)
def genecards_list_parse(soup):
    body = soup.find('body', id = "genecards")
    wrap = body.find('div', id = "wrap")
    padding = wrap.find('div', id = "mobile-padding-wrapper")
    search = padding.find('div', id = "searchResultsContainer")
    tbody = search.find('tbody')
    tr_list = tbody.find_all('tr')
    lop_result = []
    for tr in tr_list:
        td_list = tr.find_all('td')
        part_result = "\t".join([td.get_text().strip() for td in td_list])
        lop_result.append(part_result)
    lop_join_result = "\n".join(lop_result)
    return lop_join_result
def genecards_gene_parse(soup):
    print("not supported")
def get_html_Keep(driver, url):
    driver.get(url)
    pages = driver.page_source
    return pages
def get_html_restart(url):
    random_choice = [0,1] #有几个选项
    my_choice = random.choice(random_choice) #如果更多选项，下面应该选择函数列表
    my_choice = 1
    driver = web_list[my_choice](options=options_list[my_choice])
    random_TIMEOUT = random.randint(TIMEOUT-4,TIMEOUT+4)
    time.sleep(random_TIMEOUT) #增加程序等待时间减少IO压力
    driver.get(url)
    pages = driver.page_source
    time.sleep(random_TIMEOUT)
    driver.close()
    return pages
def parse_html(pages, parse_type):
    soup = BeautifulSoup(pages, 'html.parser')
    extract_result = parse_type(soup)
    #extract_json = json.dumps(extract_result, indent = 4)
    #多层级参数传入时，可以改写为可变变量, 可以全局调用
    lop_num = add_to_loop_num()
    print(f"---lop_num {lop_num} is runing---")
    return extract_result
def add_to_loop_num():
    global loop_num
    loop_num += 1
    return loop_num
#%%
#parameter
WorkSpace = "C:\\Users\\maxizheng\\Desktop\\2023_09\\NCBI_GSE" #工作目录
yaml_file = "Pycharm_ncbi_gse_p2.yaml" #yaml配置文件
result_dir = "./loop_result_p2/" #输出结果目录
if __name__ == '__main__':
    os.chdir(WorkSpace)
    yaml_data = parse_yaml(yaml_file) #解析yaml文件中的配置信息
    distribute_yaml(yaml_data) #将配置信息中的信息分配给全局参数
    range_list ,url_list = get_url_list(yaml_data) #根据yaml中 URL信息得到 url名称列表和url地址列表
    Count_initialization() #将循环数，写入数初始化，并处理开始循环数不为0的情况
    seg_range_list = [range_list[i:i+mission_num] for i in range(0, len(range_list), mission_num)]
    seg_url_list = [url_list[i:i+mission_num] for i in range(0, len(url_list), mission_num)]#将任务列表以mission_num 切分
    time_start = time.perf_counter() #记录程序开始时间
    #with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor: #打开concurrent 线程池
    executor = concurrent.futures.ThreadPoolExecutor()
        # 提交多个任务给线程池执行
        #提交给运行池后，运行的顺序不确定，故进程运行需要互不影响#
        #futures = [executor.submit(pycharm_task, url_list[url_index], Keep_on) for url_index in range(len(url_list))] 
    while dilivery_time * dilivery_once < len(seg_url_list) and dilivery_time < dilivery_end:
        dilivery_time += 1
        dilivery_range_list = seg_range_list[dilivery_once*(dilivery_time - 1):dilivery_once * dilivery_time]
        dilivery_url_list = seg_url_list[dilivery_once*(dilivery_time - 1):dilivery_once * dilivery_time]
        #[print(f"{pycharm_task}, {dilivery_range_list[url_index]}, {dilivery_url_list[url_index]}, {keep}, {webdriver}, {parse}, {download_html}") for url_index in range(len(dilivery_url_list))]
        futures = [executor.submit(pycharm_task, dilivery_range_list[url_index], dilivery_url_list[url_index], keep, webdriver, parse, download_html) for url_index in range(len(dilivery_url_list))]
        completed_url_list = [] #记录已完成的url， 帮助退出时记录未完成的url_list 5*dilivery_once
        timer = threading.Timer(dilivery_once*mission_num*(12+TIMEOUT), alarm_executor) #设置退出计时器，正常完成时间为投递数量*3
        # 获取并处理任务的结果
        try :
            timer.start()
            for future in concurrent.futures.as_completed(futures):
                url_index = futures.index(future)
                url_future = dilivery_url_list[url_index]
                result = future.result() #future结果获取超时 timeout = TIMEOUT
                write(result, storage_num)
                completed_url_list.append(url_future)
                print(f"url: {url_future}\nresult is written")
            concurrent.futures.wait(futures) #如果不增加此函数， 将不会完全等待上述过程完成就会运行下面的函数
            timer.cancel()
        except concurrent.futures.TimeoutError:#任务超时
            future.cancel()
            print(f"{future} cancel because of time out {url_future}")
        except TimeoutError: #该loop 超时，结算未完成url
            print(f"loop cancel because of time out, uncompleted url will be written in result")
        except Exception as e:
            #dilivery_item_list = [item for sublist in dilivery_url_list for item in sublist]
            #completed_item_list = [item for sublist in completed_url_list for item in sublist]
            #uncompleted_item_list = list(set(dilivery_item_list) - set(completed_item_list))
            print(f"loop cancel because of time out, uncompleted url will be written in result")
        else:
            print("loop operating right, will summarize the result")
        finally:
            for url in dilivery_url_list: #总结未获得结果的url，写入到同一个结果中
                if url not in completed_url_list:
                    write("\n".join([u+" occur wrong!" for u in url]), storage_num)
                    print(f"---check some uncompleted result! {url}")
                else:
                    print(f"---check the result correct! {url}")
            run_time = time.perf_counter() - time_start
            print(f"\n\nDilivery Time {dilivery_time} Works Done!\nThe script takes {format(run_time/60, '.2f')} min now\n\n")
            Switch = input_with_timeout("Parse Switch", 10, yaml_file)
            time.sleep(30) #等待一分钟，增加提示效果。 并且检查系统性能，检查句柄泄露等问题
            if not Switch:
                print(f"\n----------notice----------\nThe shell will turn off, close file\n\
if you want to go on with the result, you can Change dilivery_time to {dilivery_time};\n\
----------notice----------")
                break 
    else: #判断是结束节点 还是循环完成
        if dilivery_time == dilivery_end:
            print(f"dilivery_end : {dilivery_end}; you can Change dilivery_time to {dilivery_time};")
        else:
            print(f"loop_end : {dilivery_end}")
        executor.shutdown(wait=True)
    if 'RF' in globals() and not RF.closed:
        RF.close() #关闭最后一个文件
    run_time = time.perf_counter() - time_start
    print(f"The shell Work done! \nThe script takes {format(run_time/60, '.2f')} min Total")