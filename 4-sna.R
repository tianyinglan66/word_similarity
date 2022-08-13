#cd /usr/local/hadoop/spark/bin
#./sparkR --master yarn --num-executors 100  --executor-memory  3G  --driver-memory 10G

#load sna
library(Matrix)
library(igraph)
library(sna)
library(stringr)


sql('use jkgj_log')

#raw_data <- sql("select * from word_similar_dict where word not rlike '^\\d' and similar not rlike '^\\d'")
raw_data <- sql('select * from tag_similar_20161209')
raw_data1 <- repartition(raw_data,numPartitions=50)
nrow(raw_data1)
#nrow(unique(raw_data[,c("word", "similar")]))

#删除字符、数字等字符串
raw_data2<- as.data.frame(raw_data1)
raw_data3 <- as.data.frame(lapply(raw_data2, function(x) if(is.character(x)|is.factor(x)) gsub('[1-9a-zA-Z]','',x) else x))
raw_data4 <- na.omit(unique(raw_data3))
nrow(raw_data4)


#key_word <- sql("select distinct word from word_similar_dict where word not rlike '^\\d' and similar not rlike '^\\d'")
key_word <- sql('select * from tag_similar_dict_unique_word')
key_word <- unique(key_word)
key_word1 <- repartition(key_word,numPartitions=50)
nrow(key_word1)
length(unique(as.data.frame(key_word1)$word))
key_word2 = unique(as.data.frame(key_word1)$word)
key_word3 = gsub('[1-9a-zA-Z]','',key_word2)

#创建图谱,vertices = key_word3
net = graph_from_data_frame(as.data.frame(raw_data4),directed=T )

G<- simplify(net)

#随机游走
member <- walktrap.community(G)
system.time(com <-walktrap.community(G, steps = 6))##average.path.length(G) ＝6

#G_com <- community.to.membership(G, com$merges, steps=which.max(com$modularity)-1)
x <- which.max(com$membership)
x1 <- which.max(V(G)[com$membership])

for (i in 1:5){
	subg1<-induced.subgraph(G, which(com$membership==i))
    print(subg1)
    print(subg1[[1]])
    print(rownames(summary(subg1[[1]])))
}


#打印每个群组的关键词
result = data.frame(row.names=c("group_id","center_word","word"))
for (i in 1:519){
  subg1<-induced.subgraph(G, which(com$membership==i))
  center_word = rownames(summary(subg1[[1]]))
  test = data.frame(c(i),center_word,com[i])
  names(test)<-c("group_id","center_word","word")
  result = rbind(result,test)
}



#write.table(result
#               ,"/home/rd/cy/R/output/tag_dict_20161220.txt"
#               ,sep = ","
#               ,quote=FALSE
#               ,row.names=FALSE
#               ,col.names=TRUE)

sql('drop table if exists tag_dict_20161220')
sql('create table if not exists tag_dict_20161220 
    (group_id int, 
    center_word string,
    word string)')

test =createDataFrame(result)
#create tmp table
createOrReplaceTempView(test,"table_tmp")

sql('insert into table tag_dict_20161220 select * from table_tmp')



#result = data.frame(row.names =c("group_id","word"))
#for (i in 1:519){
#	test = data.frame(c(i),com[i])
#	names(test)<-c("group_id","word")
#    result = rbind(result,test)
#}

#write.df(result,'/home/rd/cy/R/output/tag_dict_20161212.txt')

#write.table(result
#               ,"/home/rd/cy/R/output/tag_dict_20161212.txt"
#               ,sep = ","
#               ,quote=FALSE
#               ,row.names=FALSE
#               ,col.names=TRUE)


V(G)$bte = betweenness(G, directed = F)


## subgroup
V(G)$sg = com$membership + 1
V(G)$color = rainbow(max(V(G)$sg))[V(G)$sg]
png(file='/home/rd/cy/R/output/igraph_subgroup.png', width = 500, height = 500)
par(mar = c(0, 0, 0, 0))
set.seed(14)
plot(G, layout = layout.fruchterman.reingold, vertex.size = 5,
    vertex.color = V(G)$color, vertex.label = NA, edge.color = grey(0.5),
    edge.arrow.mode = "-")
dev.off()





#边的中介度聚类
system.time(ec <- edge.betweenness.community(G))



#  特征值
system.time(lc <- label.propagation.community(G))





merges(com)


cfg <- cluster_fast_greedy(as.undirected(G))


zz<-file("/home/rd/R/output/child_word_20161208.txt","w")
cat(induced.subgraph(G),file=zz,sep="\n")
close(zz)



#ceb <- cluster_edge_betweenness(net) 

##计算边距
dg=degree(G)



#画出图谱
sapply(unique(V(G)), function(g) {
    subg1<-induced.subgraph(G, which(com$membership==g)) #membership id differs for each cluster
    print(subg1)
    #ecount(subg1)/ecount(G)
})

#快速聚类
cfg <- cluster_fast_greedy(as.undirected(G))
oc <- cluster_optimal(G)
x <- which.max(V(G)[com$membership])




##组别
V(G)$sg = com$membership + 1
subgroup = split(raw_data, com$membership)

clique_num(net)
cliques(net, min=17)

#打印每组结果
V(G)[com$membership==1]

## Community structure
fc <- fastgreedy.community(g)

## Create community graph, edge weights are the number of edges
cg <- contract.vertices(g, membership(fc))
E(cg)$weight <- 1
cg2 <- simplify(cg, remove.loops=FALSE)

## Plot the community graph
plot(cg2, edge.label=E(cg2)$weight, margin=.5, layout=layout.circle)



