Gem::Specification.new do |s|
  s.name        = "msmqcache"
  s.version     = "1.0.0"
  s.summary     = "Client for mcache-server (MSMQ-backed cache and queue server)"
  s.description = "Zero-dependency Ruby client with pipelines and native MSMQ queue commands."
  s.authors     = ["Hadi Cahyadi"]
  s.email       = "cumulus13@gmail.com"
  s.license     = "MIT"
  s.homepage    = "https://github.com/cumulus13/msmq-cache"
  s.required_ruby_version = ">= 3.0"
  s.files       = Dir["lib/**/*.rb", "README.md"]
  s.require_paths = ["lib"]
end
