for j in $(seq 145 1 10000)
do
    filename=tweets$j.txt
    echo $j
    if nc -zw1 api.twitter.com 443; then
        curl -X GET "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=lang" -H "Authorization: Bearer $BEARER_TOKEN" > $filename -m 21600
    fi
    sleep 5
done

