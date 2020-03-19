import * as React from "react";
import Grid from "@material-ui/core/Grid";
import HeaderBar from "./HeaderBar";
import NavigationBar from "./NavigationBar";
import CssBaseline from "@material-ui/core/CssBaseline";
import Box from "@material-ui/core/Box";
import Daily from "./Daily";
import Weekly from "./Weekly";
import { BrowserRouter as Router, Switch, Route } from "react-router-dom";

function Main() {
  return (
    <div>
      <React.Fragment>
        <CssBaseline />
        <HeaderBar />
        <Router>
          <Grid container direction="column" spacing={0}>
            <NavigationBar />
            <Grid
              container
              direction="row"
              spacing={3}
              justify={"flex-start"}
              alignItems={"flex-start"}
            >
              <Box m={5}>
                <Switch>
                  <Route path="/daily">
                    <Daily />
                  </Route>
                  <Route path="/weekly">
                    <Weekly />
                  </Route>
                </Switch>
              </Box>
            </Grid>
          </Grid>
        </Router>
      </React.Fragment>
    </div>
  );
}

export default Main;
