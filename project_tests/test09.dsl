-- Putting together all we have from Milestone 1
-- SELECT avg(col1+col2), min (col2), max(col3), avg(col3-col2), sum(col3-col2) FROM tbl2 WHERE col1 < 8 AND col2 >= -11;
s1=select(db1.tbl2.col1,null,8)
sf1=fetch(db1.tbl2.col2,s1)
s2=select(s1,sf1,-11,null)
f1=fetch(db1.tbl2.col1,s2)
f2=fetch(db1.tbl2.col2,s2)
f3=fetch(db1.tbl2.col3,s2)
add12=add(f1,f2)
out1=avg(add12)
out2=min(f2)
out3=max(f3)
sub32=sub(f3,f2)
out4=avg(sub32)
out5=sum(sub32)
print(out1,out2,out3,out4,out5)


