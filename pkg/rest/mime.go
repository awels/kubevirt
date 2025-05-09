/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package rest

const (
	MIME_ANY         string = "*/*"
	MIME_JSON        string = "application/json"
	MIME_JSON_PATCH  string = "application/json-patch+json"
	MIME_JSON_STREAM string = "application/json;stream=watch"
	MIME_MERGE_PATCH string = "application/merge-patch+json"
	MIME_YAML        string = "application/yaml"
	MIME_TEXT        string = "text/plain"
	MIME_INI         string = "text/plain"
)
