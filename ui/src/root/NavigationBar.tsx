import Tab from "@material-ui/core/Tab";
import Tabs from "@material-ui/core/Tabs";
import React, { useState } from "react";
import { Link as RouterLink } from "react-router-dom";

function NavigationBar() {
  const [selectedTab, updateSelectedTab] = useState("/daily");

  const handleTabChange = (event: React.ChangeEvent<{}>, newValue: string) => {
    updateSelectedTab(newValue);
  };

  return (
    <Tabs
      value={selectedTab}
      onChange={handleTabChange}
      indicatorColor="primary"
      textColor="primary"
    >
      <Tab
        label="Daily"
        value={"/daily"}
        component={RouterLink}
        to={"/daily"}
      />
      {/* <Tab
        label="Weekly"
        value={"/weekly"}
        component={RouterLink}
        to={"/weekly"}
      /> */}
    </Tabs>
  );
}

export default NavigationBar;
