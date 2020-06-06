import DayJsUtils from "@date-io/dayjs";
import Box from "@material-ui/core/Box";
import CssBaseline from "@material-ui/core/CssBaseline";
import Grid from "@material-ui/core/Grid";
import { MuiPickersUtilsProvider } from "@material-ui/pickers";
import React from "react";
import { BrowserRouter as Router, Route, Switch } from "react-router-dom";
import Daily from "../daily/components/Daily";
import Weekly from "../weekly/Weekly";
import HeaderBar from "./HeaderBar";
import NavigationBar from "./NavigationBar";

function Main() {
  return (
    <div>
      <React.Fragment>
        <CssBaseline />
        <MuiPickersUtilsProvider utils={DayJsUtils}>
          <HeaderBar />
          <Router>
            <Grid container direction="column" spacing={0}>
              <NavigationBar />
              <Grid
                container
                direction="row"
                spacing={3}
                justify={"center"}
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
        </MuiPickersUtilsProvider>
      </React.Fragment>
    </div>
  );
}

export default Main;
