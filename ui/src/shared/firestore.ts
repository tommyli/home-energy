import * as firebase from "firebase/app";
import "firebase/firestore";

firebase.initializeApp({
  projectId: "firefire-gcp-1"
});
const firestore = firebase.firestore();

export default firestore;
