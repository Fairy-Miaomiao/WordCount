# FBDP-作业5

邵一淼 191098180

文档内包含图片，完整文档可查看同级目录下的pdf

## 实验环境

单机使用：Java8+IntelliJ IDEA 2018.3.6

集群使用：Java7+wsl2+docker





## 需求分析

在HDFS上加载莎士比亚文集的数据文件（shakespeare-txt.zip解压后目录下的所有文件），编写MapReduce程序进行词频统计，并按照单词出现次数从大到小排列，输出（1）每个作品的前100个高频单词；（2）所有作品的前100个高频单词，要求忽略大小写，忽略标点符号（punctuation.txt），忽略停词（stop-word-list.txt），忽略数字，单词长度>=3。输出格式为"<排名>：<单词>，<次数>“，输出根据作品名称不同分别写入不同的文件。

为了获得每个作品的前个高频词和所有作品的前个高频词，需要做两类WordCount，第一类是对每个作品分别统计一次，第二类是对全部作品一起做一次。

特殊要求：忽略大小写，忽略标点符号

应对方法：在map时，读入停用词和标点文件，构建停用词、标点集合，在将单词写入文件时，与停用词、标点集合相匹配，然后剔除无关单词。

![流程](F:\FBDP\作业\流程.png)





## 设计思路

下图为类关系，箭头代表调用

![类关系](F:\FBDP\作业\类关系.png)



| 类名            | 主要实现的功能                                               |
| --------------- | ------------------------------------------------------------ |
| WordCountMain   | 主类，主要用于读入input，并对每个文件调用WordCountJob.job方法，对所有文件使用WordCountAll.job方法。 |
| WordCountJob    | 该类对输入文件进行一个词频统计的job,和一个排序的sortjob，job1中设置mapper类为WordCountMap,reducer类为WordCountReduce |
| WordCountMap    | 实现map类                                                    |
| WordCountReduce | 实现reduce类                                                 |
| WordCountSort   | 实现取前100个从大到小                                        |
| WordCountRev    | 实现sort                                                     |
| WordCountAll    | 实现对所有文件的WordCount                                    |



以下尝试用伪代码展示部分算法过程，伪代码运用不熟练，参考了一些资料，若有使用错误请批评指正。

```pseudocode
Class WordCountMap
	procedure map(docid n,doc d)
		for alll term t in doc d
			EMIT(term t,<n,1>)

class WordCountReducer
	method INITIALIZE
		t(pre) <-- null
	procedure reducer(term t, postings[<docid n1, tf1>,<docid n2, tf2>....])
		P <-- new ASSOCIATIVE_SORTED_MAP
		if t(pre) != t AND t(pre) != null
			EMIT(t, P)
			P.RESET
		for all posting<n, tf> in postings[....]
			P{n, tf} = P{n, tf++};
	method CLOSE
		EMIT(term t, P)
```







## 实验结果

实验代码及结果已上传[Fairy-Miaomiao/WordCount: Use MapReduce to do WordCount. (github.com)](https://github.com/Fairy-Miaomiao/WordCount)

实验结果可在output文件夹中查看，所有文件的前100个高频词可在output/allresult文件夹中查看。单个文件的前100个高频词可在output/top100result中寻找与文件同名txt查看。

以下展示网站截图：

![hadoop网站截图](F:\FBDP\作业\hadoop网站截图.png)





## 实验改进

1、单个文件 vs 多个文件

一开始的设计思路为对读入的每个文件做一次完整的WordCount，然后重新读入input文件夹，做一次所有的WordCount，后来发现在做所有文件词频统计时，重新读入input文件是不必要的，为了节省时间可以使用单个文件WordCount的结果。

2、扩展input文件格式

当前对于input文件夹的使用主要是，读取input文件夹下的所有文件名并形成一个list，这要求input文件夹下至少全是txt格式，为了提高适用范围，优化扩展性，可以使用递归的方法读取input每个文件夹下的txt，使input文件夹中可以文件、文件夹并存，而不是单一文件。

3、绝对路径 vs 相对路径

此问题仅在windows单机编写代码时适用。

刚开始考虑到代码在不同机器上的可使用性，所有路径表示都使用了相对路径，一般情况下不会报错，只有当进行排序job的时候，不知为何当前位置跑到了一个子目录下，有点乱套，为避免这个问题，在单机上实验的时候我将其改为了绝对路径。

同时考虑到代码的适用范围，可改进的地方为：将所有路径先在主类的main方法中申明，然后使用传参的方式传到各个类方法中。即使有绝对路径需要修改也可以一目了然。若是所有都是用相对路径不报错则更佳。



## 参考文献：

[Hadoop学习笔记—12.MapReduce中的常见算法 - EdisonZhou - 博客园 (cnblogs.com)](https://www.cnblogs.com/edisonchou/p/4299950.html)

[一些算法的MapReduce实现——倒排索引实现_iTer的专栏-CSDN博客](https://blog.csdn.net/wzhg0508/article/details/17882765)

[What does "emit" mean in general computer science terms? - Stack Overflow](https://stackoverflow.com/questions/31270657/what-does-emit-mean-in-general-computer-science-terms)

[emit - “emit”在一般计算机科学术语中是什么意思？ - IT工具网 (coder.work)](https://www.coder.work/article/7479560)

