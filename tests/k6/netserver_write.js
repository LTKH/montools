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
  var host = Math.random() * 100;
  let port = Math.floor((Math.random()+1) * 1000);
  const data = { "data": [
      { 
          "localAddr": { "ip": "127.0.0.1", "name": `host-${host}` }, 
          "remoteAddr": { "ip": "192.168.0.1", "name": "remotehost" }, 
          "relation": { "mode": "udp", "port": port}, 
          "options": {} 
      }
  ] }
  let res = http.post(`http://127.0.0.1:8084/api/v1/netmap/records`, JSON.stringify(data))

  check(res, { 'status was 204': (r) => r.status == 204 });

  sleep(0.3)
}