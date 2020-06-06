import firebase from "firebase/app";
import "firebase/auth";
import "firebase/firestore";

const config = {
  projectId: "firefire-gcp-1",
  apiKey: "AIzaSyAYUurtOMPLZRN3Rh_TCRlCQ7hAdZPiMU4",
  authDomain: "firefire-gcp-1.firebaseapp.com",
  databaseURL: "https://firefire-gcp-1.firebaseio.com",
  storageBucket: "firefire-gcp-1.appspot.com",
  messagingSenderId: "822953264136",
  appId: "1:822953264136:web:9f84b13c7ef5703efdbdbd",
};

if (!firebase.apps.length) {
  firebase.initializeApp(config);
}

const firestore = firebase.firestore();

const uiConfig = {
  signInFlow: "popup",
  signInSuccessUrl: "/daily",
  signInOptions: [firebase.auth.GoogleAuthProvider.PROVIDER_ID],
};

export { firestore as fdb, uiConfig };
