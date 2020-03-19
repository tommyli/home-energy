import * as React from "react";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";
import Grid from "@material-ui/core/Grid";
import CircularProgress from "@material-ui/core/CircularProgress";

function HeaderBar() {
  return (
    <AppBar color="primary" position="static">
      <Toolbar>
        <Grid
          container
          direction="row"
          justify="space-between"
          alignItems="center"
          alignContent="center"
        >
          <Grid item>
            <Grid container direction="row" alignItems="center" spacing={3}>
              <Grid item>
                <Typography variant="h5" color="inherit">
                  Home Energy
                </Typography>
              </Grid>
              <Grid item>
                <CircularProgress hidden={false} color="secondary" size={28} />
              </Grid>
            </Grid>
          </Grid>
        </Grid>
      </Toolbar>
    </AppBar>
  );
}

export default HeaderBar;
