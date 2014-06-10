/***********************************************************************
 PEGASUS: Peta-Scale Graph Mining System
 Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

 This software is licensed under Apache License, Version 2.0 (the  "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -------------------------------------------------------------------------
 File: ConCmpt.java
 - HCC: Find Connected Components of graph
 Version: 2.0
 ***********************************************************************/

package pegasus;

import java.io.*;

class ResultInfo {
    public long changed;
    public long unchanged;
};

public class ConCmpt {
    // read neighborhood number after each iteration.
    public static ResultInfo readIterationOutput(String new_path) throws Exception {
        ResultInfo ri = new ResultInfo();
        ri.changed = ri.unchanged = 0;
        String output_path = new_path + "/part-00000";
        String file_line = "";

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(output_path), "UTF8"));

            // Read first line
            file_line = in.readLine();

            // Read through file one line at time. Print line # and line
            while (file_line != null) {
                final String[] line = file_line.split("\t");

                if (line[0].startsWith("i"))
                    ri.changed = Long.parseLong(line[1]);
                else    // line[0].startsWith("u")
                    ri.unchanged = Long.parseLong(line[1]);

                file_line = in.readLine();
            }

            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ri;//result;
    }

}

