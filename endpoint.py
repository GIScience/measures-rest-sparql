#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from rdflib import Graph, Namespace, RDF, URIRef
from rdflib.namespace import FOAF
from urllib.parse import parse_qsl, urlparse

DQ = Namespace('http://purl.org/data-quality#')
MD = Namespace('http://www.w3.org/ns/md#')

# read settings
with open('settings.json') as f:
    settings = json.load(f)

# prepare the endpoint
def prepareEndpoint():
    # create graph
    g = Graph()

    # read main page
    r = g.parse(settings['repositoryUrl'])

    # add items
    for itemTypeLOD in [DQ.measure, DQ.result, DQ.context, FOAF.person]:
        for s in r.subjects(RDF.type, itemTypeLOD):
            g.parse(s)

    # remove the artificially crated triples indicating microdata
    g.remove((None, MD.item, RDF.nil))

    return g

# run the endpoint
def runEndpoint(g):
    class RequestHandler(BaseHTTPRequestHandler):
        def _perform_sparql(self, data):
            # test whether a query has been provided
            if 'query' not in data:
                # send header
                self.send_response(400)
                self.send_header('Content-type', 'text/html')
                self.end_headers()

                # send error message
                self.wfile.write('Please provide a query'.encode('utf-8'))
            else:
                # perform the query
                query = data['query']
                try:
                    result = g.query(query)
                except Exception as e:
                    # send header
                    self.send_response(400)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()

                    # send error message
                    self.wfile.write('The query raised an error: {}'.format(e).encode('utf-8'))
                else:
                    # send header
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()

                    # sned the content
                    self.wfile.write(result.serialize(format='json'))

        def do_GET(self):
            data = dict(parse_qsl(urlparse(self.path).query))
            self._perform_sparql(data)

        def do_POST(self):
            length = int(self.headers['Content-Length'])
            data = {key.decode('utf-8'): val.decode('utf-8') for key, val in dict(parse_qsl(self.rfile.read(length))).items()}
            self._perform_sparql(data)

    # start the server
    server_address = ('', settings.port)
    httpd = HTTPServer(server_address, RequestHandler)
    httpd.serve_forever()

# prepare and run the endpoint
data = prepareEndpoint()
runEndpoint(data)
