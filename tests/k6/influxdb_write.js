import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 500 },
    { duration: '10s', target: 1000 },
    { duration: '10s', target: 1500 },
    { duration: '10s', target: 2000 },
    { duration: '10s', target: 1500 },
    { duration: '10s', target: 1000 },
    { duration: '10s', target: 500 },
  ],
};

export default function () {
    let port = Math.floor((Math.random()+1) * 1000);
    var data = [];
    var max = 100;
    var min = 25;

    for (var i = 0; i < 1000; i++) {
      var cpu = Math.random() * 75 + 25;
      var mem = Math.random() * 75 + 25;
      var swap = Math.random() * 75 + 25;
      data.push(`cpu_usage,host=host-${i},cpu=cpu-total active=${cpu}`);
      data.push(`mem_used,host=host-${i} percent=${mem}`);
      data.push(`swap_used,host=host-${i} percent=${swap}`);
    }

    //lines := []string{
    //  fmt.Sprintf("cpu_usage,host=%s,cpu=cpu-total active=%v", host, rand.Intn(25)*4),
    //  fmt.Sprintf("mem_used,host=%s percent=%v", host, rand.Intn(25)*4),
    //  fmt.Sprintf("swap_used,host=%s percent=%v", host, rand.Intn(25)*4),
    //  fmt.Sprintf("disk_used,host=%s,path=/tmp percent=%v", host, rand.Intn(25)*4),
    //  fmt.Sprintf("disk_used,host=%s,path=/usr percent=%v", host, rand.Intn(25)*4),
    //  fmt.Sprintf("procstat_lookup,host=%s,application=test,instance=1 running=%v", host, rand.Intn(1)),
    //  fmt.Sprintf("filestat,host=%s,path=/test/test/test exists=%v", host, rand.Intn(1)),
    //}
    //for i := 0; i < size; i++ {
    //    lines = lines + fmt.Sprintf("cpu_usage,host=%s,cpu=cpu-total active=%v\r\n", host, rand.Intn(25)*4)
    //}
    //const data = { "data": [
    //    { 
    //        "localAddr": { "ip": "127.0.0.1", "name": "localhost" }, 
    //        "remoteAddr": { "ip": "192.168.0.1", "name": "remotehost" }, 
    //        "relation": { "mode": "udp", "port": port}, 
    //        "options": {} 
    //    }
    //] }

    let res = http.post(`http://127.0.0.1:8428/influx/write`, data.join("\n"))
  
    check(res, { 'status was 204': (r) => r.status == 204 });
  
    sleep(0.3)
}