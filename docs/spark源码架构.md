# spark源码架构
## 1. 环境准备（yarn集群）
```
(1) Driver，Executor
总体流程：
spark-submit => 
java ... org.apache.spark.deploy.SparkSubmit 提交给ResourceManager =>
某个NodeManager执行了java ... org.apache.spark.deploy.yarn.ApplicationMaster 执行了runDriver() 启动了Driver线程 =>
Driver同ResourceManager建立通信，注册资源，在其他NodeManager执行了java ... org.apache.spark.executor.YarnCoarseGrainedExecutorBackend 启动了ExecutorBackend进程 =>

程序运行起点：
spark-submit ... => 
java ... org.apache.spark.deploy.SparkSubmit
SparkSubmit.main(args) => 
    doSubmit(args) => 
        appArgs = parseArguments(args) => 
            new SparkSubmitArguments(args) => 
                parse(args.asJava) => 
                    Java正则表达式匹配获取名称和值 => 
                    handel(opt,value) 根据指定的名称赋值给指定的属性 => 
        appArgs.action如果没有赋值的情况下默认给SUBMIT属性 =>
        submit() =>
            判断是否standAlone模式，否 => 
            doRunMain() => 
                runMain() =>
                    childMainClass = prepareSubmitEnvironment(args) =>
                        yarn模式为org.apache.spark.deploy.yarn.YarnClusterApplication => 
                    mainClass = Utils.classForName(childMainClass) 通过childMainClass的名称获取YarnClusterApplication的信息 =>
                    app = mainClass.getConstructor() .newInstance() .asInstanceOf[SparkApplication]通过反射创建一个YarnClusterApplication对象 =>
                    app.start() =>
                        new Client() 其中包含一个YarnClient类型的属性，这个属性中又有个rmClient类型的属性 =>
                        Client.run() => 
                            submitApplication() =>
                                yarnClient.start() 和ResourceManager建立通信 => 
                                newApp = yarnClient.createApplication() 告诉yarn创建应用 => 
                                newAppResponse = newApp.getNewApplicationResponse() 获得yarn创建应用的响应 =>
                                containerContext = createContainerLaunchContext(newAppResponse) 创建容器启动环境,其中包含一些java虚拟机的配置并生成了java ... org.apache.spark.deploy.yarn.ApplicationMaster指令commands，最终包装在一个容器对象amContainer中 =>
                                appContext = createApplicationSubmissionContext(newApp, containerContext) 创建提交环境（应用的一些配置参数包括包装了java 指令的容器） =>
                                yarnClient.submitApplication(appContext) 把指令提交给了ResouceManager，最终ResourceManager在一个NodeManager中执行了java ... org.apache.spark.deploy.yarn.ApplicationMaster => 
                                ApplicationMaster.main() =>
                                    master = new ApplicationMaster(amArgs, sparkConf, yarnConf) ApplicationMaster类中有一个YarnRMClient对象属性，其中又有一个AMRMClient对象属性，是AM与RM访问的客户端
                                    master.run() =>
                                        runDriver() =>
                                            userClassThread = startUserApplication() =>
                                                mainMethod = userClassLoader.loadClass(args.userClass) .getMethod("main", classOf[Array[String]]) 根据--class参数反射创建用户提交的Object，获取到main方法
                                                new Thread("Driver") 定义Driver线程并启动 =>
                                                    mainMethod.invoke() 执行提交程序的main方法 =>
                                                        new SparkContext() 启动spark上下文 =>
                                                    sparkContextPromise.trySuccess(null) 启动spark上下文成功 =>
                                            registerAM() 向ResourceManager注册AM，申请资源 =>
                                            createAllocator() =>
                                                allocator = client.createAllocator() 定义资源分配器
                                                allocator.allocateResources() 分配资源
                                                    allocatedContainers = allocateResponse.getAllocatedContainers() 获得可分配的容器 =>
                                                    handleAllocatedContainers(allocatedContainers) 处理可用与分配的容器 =>
                                                        根据节点地址分类机架信息 =>
                                                        runAllocatedContainers(containersToUse) 启动已分配容器 =>
                                                            for (container <- containersToUse) 遍历可使用的容器 =>
                                                                launcherPool.execute(new ExecutorRunnable().run()) 通过线程池启动ExecutorRunnable的run方法 =>
                                                                    nmClient = NMClient.createNMClient() 创建NodeManager客户端 =>
                                                                    nmClient.start() =>
                                                                    startContainer() 启动容器 =>
                                                                        commands = prepareCommand() 准备指令 =>
                                                                            java ... org.apache.spark.executor.YarnCoarseGrainedExecutorBackend =>
                                                                        nmClient.startContainer(container.get, ctx) 在分配的容器中执行指令启动 =>
                                                                        YarnCoarseGrainedExecutorBackend.main() =>
                                                                            run() =>
                                            userClassThread.join() 加入线程池 =>
```


## 2. 组件通信
    (1) Driver => Executor
    (2) Executor => Driver
    (3) Executor => Executor

## 3. 应用程序的执行
    (1) 阶段的划分
    (2) 任务的切分
    (3) 任务的调度
    (4) 任务的调度
    (5) 任务的执行

## 4. Shuffle
    (1) Shuffle的原理和执行过程
    (2) Shuffle的写磁盘
    (2) Shuffle的读磁盘

## 5. 内存的管理
    (1) 内存的分类
    (2) 内存的配置