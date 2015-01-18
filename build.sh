#!/bin/bash

VERSION=0.0.1.0

rm -rf target
mkdir target

SCRATCH_DIR=hive2mysql-$VERSION

cd target
mkdir $SCRATCH_DIR

# 在这里将需要发布的文件，放到scratch目录下
cp -r ../lib ../bin ../conf  $SCRATCH_DIR
cp ../build.sh $SCRATCH_DIR
find $SCRATCH_DIR -name '*.sh' -exec chmod +x {} \;
find $SCRATCH_DIR -name '*.py' -exec chmod +x {} \;
chmod +x $SCRATCH_DIR/bin/*

# 添加log目录
mkdir $SCRATCH_DIR/log

# 删除svn目录
find . -name '.svn' -exec rm -rf {} \; 2>/dev/null
find . -name *~ -exec rm -rf {} \; 2>/dev/null


tar czf $SCRATCH_DIR.tar.gz $SCRATCH_DIR

rm -rf $SCRATCH_DIR

