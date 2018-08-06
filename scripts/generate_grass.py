from neo4j.v1 import GraphDatabase
import math

driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))

base_css = {}
base_css["node"] = {
    "diameter": "50px",
    "color": "#A5ABB6",
    "border-color": "#9AA1AC",
    "border-width": "2px",
    "text-color-internal": "#FFFFFF",
    "font-size": "10px"
}

base_css["relationship"] = {
    "color": "#A5ABB6",
    "text-color-external": "#000000",
    "text-color-internal": "#FFFFF",
    "shaft-width": "1px",
    "font-size": "8px",
    "padding": "3px",
    "caption": "\"{type}\"",
}

# node.Set_6 {
#   defaultCaption: "<id>";
#   color: #68BDF6;
#   border-color: #5CA8DB;
#   text-color-internal: #FFFFFF;
#   caption: "{id}";
# }

css = base_css

# with driver.session() as session:
#     result = session.run("""\
#     MATCH (n)
#     RETURN id(n) AS id, n.pagerank AS pagerank, n.centrality AS centrality
#     ORDER BY id
#     """)
#
#     scores = [row["pagerank"] for row in result]
#     norm = [math.sqrt(i) for i in scores]
#
#     # norm = [float(i) / max(scores) for i in scores]
#     # norm = [float(i) / sum(scores) for i in scores]
#
#     for index, score in enumerate(norm):
#         css["node.Set_{0}".format(index)] = {
#             "diameter": "{0}px".format(int(score * 100)),
#             "color": "#68BDF6",
#             "border-color": "#5CA8DB",
#             "text-color-internal": "#FFFFFF",
#             "caption": "\"{id}\"",
#             "font-size": "10px"
#         }
#
#
# with open("/tmp/pagerank.grass", "w") as css_file:
#
#     for key in css:
#         css_file.write("%s {\n" % format(key))
#         for item in css[key]:
#             css_file.write("\t{0}: {1};\n".format(item, css[key][item]))
#         css_file.write("}\n")
#
# css = base_css
#
# with driver.session() as session:
#     result = session.run("""\
#     MATCH (n)
#     RETURN id(n) AS id, n.pagerank AS pagerank, n.centrality AS centrality
#     ORDER BY id
#     """)
#
#     scores = [row["centrality"] for row in result]
#     norm = [0.5 if math.sqrt(i) < 0.5 else math.sqrt(math.sqrt(math.sqrt(i))) for i in scores]
#     print(scores, norm)
#
#     # norm = [float(i) / max(scores) for i in scores]
#     # norm = [float(i) / sum(scores) for i in scores]
#
#     for index, score in enumerate(norm):
#         css["node.Set_{0}".format(index)] = {
#             "diameter": "{0}px".format(int(score * 100)),
#             "color": "#68BDF6",
#             "border-color": "#5CA8DB",
#             "text-color-internal": "#FFFFFF",
#             "caption": "\"{id}\"",
#             "font-size": "10px"
#         }
#
#
# with open("/tmp/bw.grass", "w") as css_file:
#
#     for key in css:
#         css_file.write("%s {\n" % format(key))
#         for item in css[key]:
#             css_file.write("\t{0}: {1};\n".format(item, css[key][item]))
#         css_file.write("}\n")


with driver.session() as session:
    result = session.run("""\
    MATCH (n)
    RETURN id(n) AS id,  n.centrality_lb AS centrality
    ORDER BY id
    """)

    scores = [row["centrality"] for row in result]
    norm = [0.5 if math.sqrt(i) < 0.5 else math.sqrt(math.sqrt(math.sqrt(i))) for i in scores]
    print(scores, norm)

    # norm = [float(i) / max(scores) for i in scores]
    # norm = [float(i) / sum(scores) for i in scores]

    for index, score in enumerate(norm):
        css["node.Set_{0}".format(index)] = {
            "diameter": "{0}px".format(int(score * 80)),
            "color": "#68BDF6",
            "border-color": "#5CA8DB",
            "text-color-internal": "#FFFFFF",
            "caption": "\"{id}\"",
            "font-size": "10px"
        }


with open("/tmp/bw_lb   .grass", "w") as css_file:

    for key in css:
        css_file.write("%s {\n" % format(key))
        for item in css[key]:
            css_file.write("\t{0}: {1};\n".format(item, css[key][item]))
        css_file.write("}\n")

# css = base_css
#
# with driver.session() as session:
#     result = session.run("""\
#     MATCH (n)
#     RETURN id(n) AS id, n.closeness AS score
#     ORDER BY id
#     """)
#
#     scores = [row["score"] for row in result]
#     print(scores)
#     norm = [0.5 if score < 0.5 else score for score in scores]
#
#     # norm = [float(i) / max(scores) for i in scores]
#     # norm = [float(i) / sum(scores) for i in scores]
#
#     for index, score in enumerate(norm):
#         css["node.Set_{0}".format(index)] = {
#             "diameter": "{0}px".format(int(score * 80)),
#             "color": "#68BDF6",
#             "border-color": "#5CA8DB",
#             "text-color-internal": "#FFFFFF",
#             "caption": "\"{id}\"",
#             "font-size": "10px"
#         }
#
#
# with open("/tmp/cc.grass", "w") as css_file:
#
#     for key in css:
#         css_file.write("%s {\n" % format(key))
#         for item in css[key]:
#             css_file.write("\t{0}: {1};\n".format(item, css[key][item]))
#         css_file.write("}\n")
#
# css = base_css
#
# with driver.session() as session:
#     result = session.run("""\
#     MATCH (n)
#     RETURN id(n) AS id, n.alt_closeness AS score
#     ORDER BY id
#     """)
#
#     scores = [row["score"] * 4.5 for row in result]
#     print(scores)
#     norm = [0.5 if score < 0.5 else math.sqrt(score) for score in scores]
#
#     # norm = [float(i) / max(scores) for i in scores]
#     # norm = [float(i) / sum(scores) for i in scores]
#
#     for index, score in enumerate(norm):
#         css["node.Set_{0}".format(index)] = {
#             "diameter": "{0}px".format(int(score * 80)),
#             "color": "#68BDF6",
#             "border-color": "#5CA8DB",
#             "text-color-internal": "#FFFFFF",
#             "caption": "\"{id}\"",
#             "font-size": "10px"
#         }
#
#
# with open("/tmp/alt_cc.grass", "w") as css_file:
#
#     for key in css:
#         css_file.write("%s {\n" % format(key))
#         for item in css[key]:
#             css_file.write("\t{0}: {1};\n".format(item, css[key][item]))
#         css_file.write("}\n")
#
#
# css = base_css
#
# with driver.session() as session:
#     result = session.run("""\
#     MATCH (n)
#     RETURN id(n) AS id, n.followers AS score
#     ORDER BY id
#     """)
#
#     scores = [row["score"] for row in result]
#     print(scores)
#     norm = [0.5 if math.sqrt(i) < 0.5 else math.sqrt(math.sqrt(i)) for i in scores]
#
#     # norm = [float(i) / max(scores) for i in scores]
#     # norm = [float(i) / sum(scores) for i in scores]
#
#     for index, score in enumerate(norm):
#         css["node.Set_{0}".format(index)] = {
#             "diameter": "{0}px".format(int(score * 80)),
#             "color": "#68BDF6",
#             "border-color": "#5CA8DB",
#             "text-color-internal": "#FFFFFF",
#             "caption": "\"{id}\"",
#             "font-size": "10px"
#         }
#
#
# with open("/tmp/degree.grass", "w") as css_file:
#
#     for key in css:
#         css_file.write("%s {\n" % format(key))
#         for item in css[key]:
#             css_file.write("\t{0}: {1};\n".format(item, css[key][item]))
#         css_file.write("}\n")