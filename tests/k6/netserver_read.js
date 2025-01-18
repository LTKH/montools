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
  var host = Math.floor(Math.random() * 100);

  let res = http.get(`http://127.0.0.1:8084/api/v1/netmap/records?src_name=host-${host}`)

  check(res, { 'status was 200': (r) => r.status == 200 });
  check(res.json(), { 'retrieved alerts list': (r) => r.data.length > 0 });

  sleep(0.3)
}