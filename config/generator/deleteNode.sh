for (( i=1;i<=5; i++))
do
    kubectl delete node worker$i
done