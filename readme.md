# 学习kv数据库从0到1的搭建与实现

# bitcask基本思路：
![img_2.png](img_2.png)

![img_1.png](img_1.png)



# 类图逻辑：
## dataFile相关
![img_4.png](img_4.png)






---

## 知识空缺查漏：
ps:在此项目常使用varint编码将数字压缩存入byte数组中 n int--->x []byte

粗糙看了下varint编码原理，其实是将n每次取出低七位的数字 即byte(n)|0x80-->存入到byte[i],i=0,1,2,3....

因此byte数组存储的实质上是该数字的小端序

因此maxVarint64为10 因为7*9+1=64，所以需要10字节
依次举一反三： maxVarint32为5