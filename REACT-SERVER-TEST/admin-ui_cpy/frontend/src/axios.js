import axios from 'axios';

//base64 form for http authentication header
const auth_acc = "nginx";
const auth_pw  = "iHrpmWQ4hj2BSxACfsP2V5Uk";
var auth_header = Buffer.from(auth_acc + ":" + auth_pw).toString("base64");
auth_header     = "Basic " + auth_header;

export default axios.create({
  baseURL: 'https://elastics.northeurope.cloudapp.azure.com/',
  headers: {
    Authorization: auth_header
  }
});
