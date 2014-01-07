
Implementation details
----------------------

The graph is stored in a single table with the following schema.

<table>
<tr><th>Row</th>                                   <th>CF</th>         <th>CQ</th>              <th>Value</th>       <th>Description</th></tr>
<tr><td>DrowKey\x1fpropertyName\x1fpropertyId</td> <td>-</td>          <td>-</td>               <td>data</td>        <td>Stores the data for StreamingPropertyValue</td></tr>
<tr><td>V[id]</td>                                 <td>V</td>          <td>-</td>               <td>-</td>           <td>Vertex id</td></tr>
<tr><td>V[id]</td>                                 <td>EOUT</td>       <td>[e id]</td>          <td>[e label]</td>   <td>Vertex out-edge</td></tr>
<tr><td>V[id]</td>                                 <td>EIN</td>        <td>[e id]</td>          <td>[e label]</td>   <td>Vertex in-edge</td></tr>
<tr><td>E[id]</td>                                 <td>E</td>          <td>[e label]</td>       <td>-</td>           <td>Edge id</td></tr>
<tr><td>E[id]</td>                                 <td>VOUT</td>       <td>[v id]</td>          <td>-</td>           <td>Edge out-vertex</td></tr>
<tr><td>E[id]</td>                                 <td>VIN</td>        <td>[v id]</td>          <td>-</td>           <td>Edge in-vertex</td></tr>
<tr><td>V/E[id]</td>                               <td>PROP</td>       <td>[pname\x1fpid]</td>  <td>[pval]</td>      <td>Element property</td></tr>
<tr><td>V/E[id]</td>                               <td>PROPMETA</td>   <td>[pname\x1fpid]</td>  <td>[pmetadata]</td> <td>Element property metadata</td></tr>
</table>
