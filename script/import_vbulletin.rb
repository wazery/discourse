require "rubygems"
require "bundler/setup"

Bundler.require(:import)

require "parallel"

################################################################################

def log(message)
  puts "[#{Time.now}] #{message}"
end

# used to clean the dumps
def clean_dump(path)
  lines = []
  File.open(path, "r:iso-8859-1").each_line do |line|
    lines << line.gsub("\r", "\\r")
  end

  File.open(path, "w:iso-8859-1") { |f| f.puts lines }
end

def reconnect
  # WTF! This needs to be done twice (cf. https://github.com/grosser/parallel/issues/62#issuecomment-20538395)
  ActiveRecord::Base.connection.reconnect! rescue nil
  ActiveRecord::Base.connection.reconnect! rescue nil
end

################################################################################

# clean db
log "cleaning the db..."
`bundle exec rake db:drop && bundle exec rake db:create && bundle exec rake db:migrate`

# load app & gems
log "loading rails..."
require File.expand_path(File.dirname(__FILE__) + "/../config/environment")
require "csv"

################################################################################

VBGroup         = Struct.new(:id, :name)
VBUser          = Struct.new(:id, :group_id, :username, :username_lower, :name, :previous_username, :email, :website, :title, :bio)
VBCategory      = Struct.new(:id, :name, :description)
VBCategoryGroup = Struct.new(:category_id, :group_id, :permissions)
VBTopic         = Struct.new(:id, :title, :slug, :user_id, :created_at, :category_id, :views, :visible, :sticky, :posts)
VBPost          = Struct.new(:id, :topic_id, :user_id, :created_at, :raw, :visible, :parent_id)

@groups = {}
@groups_mapping, @mapped_group_ids = {}, {}

@users = {}
@users_email, @users_username_lower = {}, {}
@old_username_to_new_usernames = {}

@categories = {}
@categories_mapping = {}
@mapped_categories = Set.new

@categories_groups = []

@topics = {}
@topics_per_user, @posts_per_user = {}, {}
@posts = {}

@default_csv_options = { headers: true, encoding: "iso-8859-1" }

################################################################################

def load_categories_mapping(path)
  log "Loading categories mapping..."

  CSV.foreach(path, @default_csv_options) do |line|
    id   = line["id"].to_i
    name = line["name"]

    @categories_mapping[name] ||= []
    @categories_mapping[name] << id

    @mapped_categories << id
  end

  log "Loaded #{@categories_mapping.size} categories mapping"
end

def load_categories(path)
  log "Loading categories..."

  CSV.foreach(path, @default_csv_options) do |line|
    category = VBCategory.new
    category.id          = line["forumid"].to_i
    category.name        = line["title"].strip[0...50]
    category.description = line["description"]
    @categories[category.id] = category
  end

  log "Loaded #{@categories.size} categories"
end

def load_categories_groups(path)
  log "Loading categories groups..."

  CSV.foreach(path, @default_csv_options) do |line|
    next unless permissions = translate_forum_permissions(line["forumpermissions"].to_i)

    category_group = VBCategoryGroup.new
    category_group.category_id = line["forumid"].to_i
    category_group.group_id    = line["usergroupid"].to_i
    category_group.permissions = permissions

    @categories_groups << category_group
  end

  log "Loaded #{@categories_groups.size} categories groups"
end

# FROM THE "bitfield_vbulletin.xml" FILE
VB_FORUM_PERMISSIONS_CAN_VIEW = 1
VB_FORUM_PERMISSIONS_CAN_VIEW_THREADS = 524288
VB_FORUM_PERMISSIONS_CAN_REPLY_OWN = 32
VB_FORUM_PERMISSIONS_CAN_REPLY_OTHERS = 64
VB_FORUM_PERMISSIONS_CAN_POST_NEW = 16

def translate_forum_permissions(permissions)
  can_see    = ((permissions & VB_FORUM_PERMISSIONS_CAN_VIEW) | (permissions & VB_FORUM_PERMISSIONS_CAN_VIEW_THREADS)) > 0
  can_reply  = ((permissions & VB_FORUM_PERMISSIONS_CAN_REPLY_OWN) | (permissions & VB_FORUM_PERMISSIONS_CAN_REPLY_OTHERS)) > 0
  can_create = (permissions & VB_FORUM_PERMISSIONS_CAN_POST_NEW) > 0
  return CategoryGroup.permission_types[:full]        if can_see && can_reply && can_create
  return CategoryGroup.permission_types[:create_post] if can_see && can_reply
  return CategoryGroup.permission_types[:readonly]    if can_see
  nil
end

def load_topics(path)
  log "Loading topics..."

  csv_options = @default_csv_options.merge({ col_sep: "\t", quote_char: "\u200B" })

  CSV.foreach(path, csv_options) do |line|
    topic = VBTopic.new
    topic.id          = line["threadid"].to_i
    topic.title       = line["title"].strip[0...255]
    topic.slug        = Slug.for(topic.title)
    topic.user_id     = line["postuserid"].to_i
    topic.created_at  = Time.at(line["dateline"].to_i)
    topic.category_id = line["forumid"].to_i
    topic.views       = line["views"].to_i
    topic.visible     = line["visible"].to_i == 1
    topic.sticky      = line["sticky"].to_i == 1
    topic.posts       = []
    # do not migrate topics that aren't mapped
    if @mapped_categories.size > 0 && !@mapped_categories.include?(topic.category_id)
      log "topic ##{topic.id} (#{topic.title}) was not migrated because it belongs to an unmapped category (##{topic.category_id})"
      next
    end
    # add the topic
    @topics[topic.id] = topic
    # update some counters
    @topics_per_user[topic.user_id] ||= 0
    @topics_per_user[topic.user_id] += 1
  end

  log "Loaded #{@topics.size} topics"
end

def load_posts(path)
  log "Loading posts..."

  csv_options = @default_csv_options.merge({ col_sep: "\t", quote_char: "\u200B" })
  post_count = 0

  CSV.foreach(path, csv_options) do |line|
    topic_id = line["threadid"].to_i
    # make sure the topic exists
    next unless @topics.has_key?(topic_id)
    # create post
    post = VBPost.new
    post.id         = line["postid"].to_i
    post.topic_id   = topic_id
    post.user_id    = line["userid"].to_i
    post.created_at = Time.at(line["dateline"].to_i)
    post.raw        = (line["pagetext"] || "").gsub(/(\\r)?\\n/, "\n").gsub("\\t", "\t")
    post.visible    = line["visible"].to_i == 1
    post.parent_id  = line["parentid"].to_i
    # update some counters
    @posts_per_user[post.user_id] ||= 0
    @posts_per_user[post.user_id] += 1
    # add the post
    @topics[post.topic_id].posts << post
    post_count += 1
  end

  log "Loaded #{post_count} posts"
end

def load_groups_mapping(path)
  log "Loading groups mapping..."

  CSV.foreach(path, @default_csv_options) do |line|
    old_id = line["old_id"].to_i
    new_id = line["new_id"].to_i

    @groups_mapping[new_id] ||= []
    @groups_mapping[new_id] << old_id

    @mapped_group_ids[old_id] = true
    @mapped_group_ids[new_id] = true
  end

  log "Loaded #{@groups_mapping.size} groups mapping"
end

def load_groups(path)
  log "Loading groups..."

  CSV.foreach(path, @default_csv_options) do |line|
    group = VBGroup.new
    group.id   = line["usergroupid"].to_i
    group.name = line["title"].gsub(/[^A-Za-z0-9_]/, "").strip
    @groups[group.id] = group
  end

  log "Loaded #{@groups.size} groups"
end

################################################################################
# HAS BEEN EXTRACTED FROM THE UserNameSuggester                                #
################################################################################

def suggest(name)
  return unless name.present?
  find_available_username_based_on(name)
end

def find_available_username_based_on(name)
  name = rightsize_username(sanitize_username!(name))
  i = 1
  attempt = name
  while @users_username_lower.has_key?(attempt.downcase)
    suffix = i.to_s
    max_length = User.username_length.end - suffix.length - 1
    attempt = "#{name[0..max_length]}#{suffix}"
    i += 1
  end
  attempt
end

def sanitize_username!(name)
  name = ActiveSupport::Inflector.transliterate(name)
  name.gsub!(/^[^[:alnum:]]+|\W+$/, "")
  name.gsub!(/\W+/, "_")
  name.gsub!(/^\_+/, '')
  name
end

def rightsize_username(name)
  name.ljust(User.username_length.begin, '1')[0, User.username_length.end]
end

################################################################################

def load_users(path)
  log "Loading users..."

  csv_options = @default_csv_options.merge({ col_sep: "\t", quote_char: "\u200B" })

  CSV.foreach(path, csv_options) do |line|
    user = VBUser.new
    user.id                = line["userid"].to_i
    user.group_id          = line["usergroupid"].to_i
    user.name              = line["username"]
    user.previous_username = line["username"]
    user.username          = suggest(line["username"])
    user.username_lower    = user.username.downcase
    user.email             = line["email"]
    user.website           = line["homepage"]
    user.title             = line["usertitle"]
    user.bio               = line["field1"]
    # some checks
    next if user_is_invalid?(user)
    # add user to the list
    @users[user.id] = user
    # keep indexes up to date
    @users_email[user.email] = user.id
    @users_username_lower[user.username_lower] = user.id
    @old_username_to_new_usernames[user.previous_username] = user.username
  end

  log "Loaded #{@users.size} users"
end

def user_is_invalid?(user)
  user_details = "user ##{user.id} (topics: #{@topics_per_user[user.id] || 0}, posts: #{@posts_per_user[user.id] || 0})"
  if user.email.blank?
    log "#{user_details} is missing an email address"
    return true
  end
  if @users_email.has_key?(user.email)
    log "#{user_details} was not imported because another user (##{@users_email[user.email]}) has the same email address (#{user.email})"
    return true
  end
  if user.previous_username != user.username
    log "#{user_details} had his/her username changed from \"#{user.previous_username}\" to \"#{user.username}\" (#{user.email})"
  end
  false
end

################################################################################

def create_groups
  log "Creating groups..."

  if @groups_mapping.size > 0
    # reject unmaped groups
    @groups.reject! { |group_id, group| !@mapped_group_ids.has_key?(group_id) }
    # create groups and correctly map generated ids
    @groups_mapping.each do |group_id, mapped_group_ids|
      group = @groups[group_id]
      group.id = create_group(group.name)
      mapped_group_ids.each do |mapped_group_id|
        @groups[mapped_group_id].id = group.id
      end
    end
  else
    @groups.each do |group_id, group|
      begin
        group.id = create_group(group.name)
      rescue ActiveRecord::RecordNotUnique
        log "group ##{group.id} (#{group.name}) already exists..."
      end
    end
  end
end

def create_group(name)
  result = Group.exec_sql("INSERT INTO groups (name) VALUES (:name) RETURNING id", name: name)
  # return new id
  result.first["id"].to_i
end

def create_users
  log "Creating users..."

  @users.values.in_groups_of(10_000, false) do |users|
    values = users.map { |user| sql_fragment_for_user(user) }
    sql = "INSERT INTO users (username, username_lower, name, email, website, title, primary_group_id, trust_level, email_digests, external_links_in_new_tab, bio_raw)
           VALUES #{values.join(",")}
           RETURNING id"
    results = ActiveRecord::Base.connection.raw_connection.async_exec(sql).to_a
    # update the ids
    results.each_with_index do |result, index|
      users[index].id = result["id"].to_i
    end
  end
end

def sql_fragment_for_user(user)
  User.sql_fragment("(:username, :username_lower, :name, :email, :website, :title, :primary_group_id, :trust_level, :email_digests, :external_links_in_new_tab, :bio_raw)",
                     username: user.username,
                     username_lower: user.username_lower,
                     name: user.name,
                     email: user.email,
                     website: user.website,
                     title: user.title,
                     primary_group_id: @groups.has_key?(user.group_id) ? @groups[user.group_id].id : nil,
                     trust_level: 1,
                     email_digests: false,
                     external_links_in_new_tab: false,
                     bio_raw: user.bio)
end

def create_groups_membership
  log "Creating groups membership..."

  @groups.each do |group_id, group|
    # list users
    user_ids_in_group = @users.keys.select { |user_id| @users[user_id].group_id == group_id }
    next if user_ids_in_group.size == 0
    # add users to group
    values = user_ids_in_group.map { |user_id| "(#{@groups[group_id].id}, #{@users[user_id].id})" }.join(",")
    sql = "BEGIN; INSERT INTO group_users (group_id, user_id) VALUES #{values}; COMMIT;"
    ActiveRecord::Base.connection.raw_connection.async_exec(sql)
  end
end

def create_categories
  log "Creating categories..."

  if @categories_mapping.size > 0
    # create the categories from the mapping
    @categories_mapping.each do |name, category_ids|
      id = create_category(name)
      category_ids.each do |category_id|
        @categories[category_id].id = id if @categories.has_key?(category_id)
      end
    end
  else
    # create the categories from the dump
    @categories.each do |_, category|
      begin
        category.id = create_category(category.name, category.description)
      rescue ActiveRecord::RecordNotUnique, ActiveRecord::RecordInvalid
        log "category ##{category.id} (#{category.name}) already exists..."
      end
    end
  end
end

def create_category(name, definition=nil)
  category = Category.create(name: name.dup, color: "AB9364", text_color: "FFFFFF", user: Discourse.system_user)
  category.save!

  # create definition topic
  if definition
    definition_post = category.topics.first.posts.first
    definition_post.raw = definition
    definition_post.save
  end

  category.id
end

def create_categories_groups
  log "Creating categories groups..."

  @categories_groups.each do |category_group|
    next unless @categories.has_key?(category_group.category_id)
    next unless @groups.has_key?(category_group.group_id)

    CategoryGroup.new(
      category_id: @categories[category_group.category_id].id,
      group_id: @groups[category_group.group_id].id,
      permission_type: category_group.permissions
    ).save!
  end
end

def create_topics_and_posts
  log "Creating topics and posts..."

  @valid_topic_ids = @topics.keys.select { |t| is_valid_topic?(@topics[t]) }.to_a

  @valid_topic_ids.in_groups_of(10_000, false) do |topic_ids|
    values = topic_ids.map { |topic_id| sql_fragment_for_topic(@topics[topic_id]) }
    sql = "INSERT INTO topics (title, slug, created_at, user_id, category_id, visible, pinned_at, views, last_posted_at, last_post_user_id, bumped_at)
           VALUES #{values.join(",")}
           RETURNING id"
    results = ActiveRecord::Base.connection.raw_connection.async_exec(sql).to_a
    # update the ids
    results.each_with_index do |result, index|
      topic_id = topic_ids[index]
      @topics[topic_id].id = result["id"].to_i
    end
    # create all the posts
    topic_ids.each { |topic_id| create_posts(@topics[topic_id], topic_id) }
  end
end

def is_valid_topic?(topic)
  topic_details = "topic ##{topic.id} (#{topic.title})"
  if topic.posts.count == 0
    log "#{topic_details} was not imported because it has no post"
    return false
  end
  unless @users.has_key?(topic.user_id)
    log "#{topic_details} was not imported because the OP (##{topic.user_id}) is missing"
    return false
  end
  true
end

def sql_fragment_for_topic(topic)
  Topic.sql_fragment("(:title, :slug, :created_at, :user_id, :category_id, :visible, :pinned_at, :views, :last_posted_at, :last_post_user_id, :bumped_at)",
                      title: topic.title,
                      slug: topic.slug,
                      created_at: topic.created_at,
                      user_id: @users[topic.user_id].id,
                      category_id: @categories[topic.category_id].id,
                      visible: topic.visible,
                      pinned_at: topic.sticky ? topic.created_at : nil,
                      views: topic.views,
                      last_posted_at: topic.created_at,
                      last_post_user_id: @users[topic.user_id].id,
                      bumped_at: topic.created_at)
end

def create_posts(topic, old_topic_id)
  post_number = 0
  # make sure the posts are properly ordered
  ordered_post_ids = topic.posts.map { |p| p.id }.sort
  # process the posts in batches of 5,000
  ordered_post_ids.in_groups_of(5_000, false) do |post_ids|
    values = post_ids.map do |post_id|
      post_number += 1
      post = topic.posts.select { |p| p.id == post_id }.first
      sql_fragment_for_post(post, topic.id, post_number)
    end
    sql = "INSERT INTO posts (topic_id, user_id, raw, cooked, post_number, sort_order, reply_to_post_number, created_at, last_version_at, last_editor_id, word_count)
           VALUES #{values.join(",")}
           RETURNING id, post_number"
    results = ActiveRecord::Base.connection.raw_connection.async_exec(sql).to_a
    # update the ids
    results.each_with_index do |result, index|
      post_id = post_ids[index]
      @posts[post_id] = { id: result["id"].to_i, post_number: result["post_number"].to_i, new_topic_id: topic.id, old_topic_id: old_topic_id }
    end
  end
end

def sql_fragment_for_post(post, topic_id, post_number)
  user_id = @users.has_key?(post.user_id) ? @users[post.user_id].id : -1

  Post.sql_fragment("(:topic_id, :user_id, :raw, :cooked, :post_number, :sort_order, :reply_to_post_number, :created_at, :last_version_at, :last_editor_id, :word_count)",
                     topic_id: topic_id,
                     user_id: user_id,
                     raw: post.raw,
                     cooked: post.raw, #cooked will be updated in post-processing phase
                     post_number: post_number,
                     sort_order: post_number,
                     reply_to_post_number: @posts.has_key?(post.parent_id) ? @posts[post.parent_id][:post_number] : nil,
                     created_at: post.created_at,
                     last_version_at: post.created_at,
                     last_editor_id: user_id,
                     word_count: post.raw.scan(/\w+/).size)
end

################################################################################

def postprocess_posts
  log "Post processing posts..."

  Parallel.each(@valid_topic_ids) do |topic_id|
    @reconnected ||= ActiveRecord::Base.connection.reconnect! || true

    updates = @topics[topic_id].posts.map do |post|
      raw = postprocess_post_raw(post.raw.dup)
      cooked = cook_post(raw)
      Post.sql_fragment("UPDATE posts SET raw = :raw, cooked = :cooked WHERE id = :id", raw: raw, cooked: cooked, id: @posts[post.id][:id])
    end

    updates.in_groups_of(5_000, false) do |u|
      sql = "BEGIN;" << u.join(";") << ";COMMIT;"
      ActiveRecord::Base.connection.raw_connection.async_exec(sql)
    end
  end

  reconnect
end

def postprocess_post_raw(raw)

  # [MENTION]<username>[/MENTION]
  raw = raw.gsub(/\[mention\](.+?)\[\/mention\]/i) do
    old_username = $1
    if @old_username_to_new_usernames.has_key?(old_username)
      username = @old_username_to_new_usernames[old_username]
      "@#{username}"
    else
      $&
    end
  end

  # [MENTION=<user_id>]...[/MENTION]
  raw = raw.gsub(/\[mention=(\d+)\].+?\[\/mention\]/i) do
    user_id = $1.to_i
    if @users.has_key?(user_id)
      username = @users[user_id].username
      "@#{username}"
    else
      $&
    end
  end

  # [QUOTE]...[/QUOTE]
  raw = raw.gsub(/\[quote\](.+?)\[\/quote\]/im) { "\n> #{$1}\n" }

  # [QUOTE=<username>]...[/QUOTE]
  raw = raw.gsub(/\[quote=([^;\]]+)\](.+?)\[\/quote\]/im) do
    old_username, quote = $1, $2
    if @old_username_to_new_usernames.has_key?(old_username)
      username = @old_username_to_new_usernames[old_username]
      "\n[quote=\"#{username}\"]\n#{quote}\n[/quote]\n"
    else
      $&
    end
  end

  # [QUOTE=<username>;<post_id>]...[/QUOTE]
  raw = raw.gsub(/\[quote=([^;]+);(\d+)\](.+?)\[\/quote\]/im) do
    old_username, post_id, quote = $1, $2.to_i, $3
    if @old_username_to_new_usernames.has_key?(old_username) && @posts.has_key?(post_id)
      post_number = @posts[post_id][:post_number]
      topic_id    = @posts[post_id][:new_topic_id]
      username    = @old_username_to_new_usernames[old_username]
      "\n[quote=\"#{username},post:#{post_number},topic:#{topic_id}\"]\n#{quote}\n[/quote]\n"
    else
      $&
    end
  end

  # [HTML]...[/HTML]
  # [PHP]...[/PHP]
  ["html", "php"].each do |language|
    raw = raw.gsub(/\[#{language}\](.+?)\[\/#{language}\]/im) { "\n```#{language}\n#{$1}\n```\n" }
  end

  # [CODE]...[/CODE]
  raw = raw.gsub(/\[code\](.+?)\[\/code\]/im) { "\n```\n#{$1}\n```\n" }

  # [HIGHLIGHT="..."]...[/HIGHLIGHT]
  raw = raw.gsub(/\[highlight(?:[^\]]*)\](.+)\[\/highlight\]/im) { "\n```\n#{$1}\n```\n" }

  # [SAMP]...[SAMP]
  raw = raw.gsub(/\[samp\](.+?)\[\/samp\]/i) { "`#{$1}`" }

  # [YOUTUBE]<id>[/YOUTUBE]
  raw = raw.gsub(/\[youtube\](.+?)\[\/youtube\]/i) { "http://youtu.be/#{$1}" }

  # [THREAD]<thread_id>[/THREAD]
  # ==> http://my.discourse.org/t/slug/<topic_id>
  raw = raw.gsub(/\[thread\](\d+)\[\/thread\]/i) do
    topic_id = $1.to_i
    if @topics.has_key?(topic_id)
      Topic.url(@topics[topic_id].id, @topics[topic_id].slug)
    else
      $&
    end
  end

  # [THREAD=<thread_id>]...[/THREAD]
  # ==> [...](http://my.discourse.org/t/slug/<topic_id>)
  raw = raw.gsub(/\[thread=(\d+)\](.+?)\[\/thread\]/i) do
    topic_id, link = $1.to_i, $2
    if @topics.has_key?(topic_id)
      url = Topic.url(@topics[topic_id].id, @topics[topic_id].slug)
      "[#{link}](#{url})"
    else
      $&
    end
  end

  # [POST]<post_id>[/POST]
  # ==> http://my.discourse.org/t/slug/<topic_id>/<post_number>
  raw = raw.gsub(/\[post\](\d+)\[\/post\]/i) do
    post_id = $1.to_i
    if @posts.has_key?(post_id)
      old_topic_id = @posts[post_id][:old_topic_id]
      post_number  = @posts[post_id][:post_number]
      Topic.url(@topics[old_topic_id].id, @topics[old_topic_id].slug, post_number)
    else
      $&
    end
  end

  # [POST=<post_id>]...[/POST]
  # ==> [...](http://my.discourse.org/t/<topic_slug>/<topic_id>/<post_number>)
  raw = raw.gsub(/\[post=(\d+)\](.+?)\[\/post\]/i) do
    post_id, link = $1.to_i, $2
    if @posts.has_key?(post_id)
      old_topic_id = @posts[post_id][:old_topic_id]
      post_number  = @posts[post_id][:post_number]
      url          = Topic.url(@topics[old_topic_id].id, @topics[old_topic_id].slug, post_number)
      "[#{link}](#{url})"
    else
      $&
    end
  end

  raw
end

def cook_post(raw)
  cooked = PrettyText.cook(raw) rescue PrettyText::JavaScriptError
  cooked || raw
end

################################################################################

def update_users_stats
  log "Updating users stats..."

  # TODO: parallelize
  User.select(:id, :bio_raw).where("length(COALESCE(bio_raw, '')) > 0").find_each do |user|
    bio_cooked = PrettyText.cook(user.bio_raw, omit_nofollow: false)
    User.exec_sql("UPDATE users SET bio_cooked = :bio_cooked WHERE id = :id", bio_cooked: bio_cooked, id: user.id)
  end

  # user actions
  reset_user_actions
  create_new_topic_user_actions
  create_reply_user_actions

  # TODO:
  # RESPONSE= 6
  # MENTION = 7
  # QUOTE = 9
end

def reset_user_actions
  log "Reseting user actions..."

  UserAction.exec_sql("TRUNCATE TABLE user_actions")
end

def create_new_topic_user_actions
  log "Creating NEW_TOPIC user actions..."

  sql = <<-SQL
    INSERT INTO user_actions (action_type, user_id, target_topic_id, target_post_id, acting_user_id, created_at)
    SELECT #{UserAction::NEW_TOPIC}, user_id, id, -1, user_id, created_at
    FROM topics
  SQL

  UserAction.exec_sql(sql)
end

def create_reply_user_actions
  log "Creating REPLY user actions..."

  sql = <<-SQL
    INSERT INTO user_actions (action_type, user_id, target_topic_id, target_post_id, acting_user_id, created_at)
    SELECT #{UserAction::REPLY}, user_id, topic_id, id, user_id, created_at
    FROM posts
    WHERE post_number > 1
  SQL

  UserAction.exec_sql(sql)
end

def update_groups_stats
  log "Updating groups stats..."

  sql = <<-SQL
    UPDATE groups g
    SET    user_count = (
      SELECT COUNT(*)
      FROM   group_users
      WHERE  group_id = g.id
    )
  SQL

  Group.exec_sql(sql)
end

def update_topics_stats
  log "Updating topics stats..."

  sql = <<-SQL
    UPDATE topics t
    SET    last_post_user_id     = (SELECT user_id                 FROM posts WHERE deleted_at IS NULL AND topic_id = t.id ORDER BY created_at DESC LIMIT 1),
           last_posted_at        = (SELECT created_at              FROM posts WHERE deleted_at IS NULL AND topic_id = t.id ORDER BY created_at DESC LIMIT 1),
           bumped_at             = (SELECT created_at              FROM posts WHERE deleted_at IS NULL AND topic_id = t.id ORDER BY created_at DESC LIMIT 1),
           posts_count           = (SELECT COUNT(*)                FROM posts WHERE deleted_at IS NULL AND topic_id = t.id),
           participant_count     = (SELECT COUNT(distinct user_id) FROM posts WHERE deleted_at IS NULL AND topic_id = t.id),
           highest_post_number   = (SELECT COUNT(*)                FROM posts WHERE deleted_at IS NULL AND topic_id = t.id) + 1,
           moderator_posts_count = (SELECT COUNT(*)                FROM posts WHERE deleted_at IS NULL AND topic_id = t.id AND post_type = #{Post.types[:moderator_action]}),
           word_count            = (SELECT SUM(word_count)         FROM posts WHERE deleted_at IS NULL AND topic_id = t.id),
           like_count            = (SELECT SUM(like_count)         FROM posts WHERE deleted_at IS NULL AND topic_id = t.id),
           bookmark_count        = (SELECT SUM(bookmark_count)     FROM posts WHERE deleted_at IS NULL AND topic_id = t.id),
           featured_user1_id     = (SELECT user_id FROM (SELECT COUNT(*) AS count_all, user_id FROM posts WHERE deleted_at IS NULL AND topic_id = t.id AND (user_id NOT IN (t.user_id, t.last_post_user_id)) GROUP BY user_id ORDER BY count_all DESC LIMIT 1 OFFSET 0) AS fu UNION (SELECT NULL) LIMIT 1),
           featured_user2_id     = (SELECT user_id FROM (SELECT COUNT(*) AS count_all, user_id FROM posts WHERE deleted_at IS NULL AND topic_id = t.id AND (user_id NOT IN (t.user_id, t.last_post_user_id)) GROUP BY user_id ORDER BY count_all DESC LIMIT 1 OFFSET 1) AS fu UNION (SELECT NULL) LIMIT 1),
           featured_user3_id     = (SELECT user_id FROM (SELECT COUNT(*) AS count_all, user_id FROM posts WHERE deleted_at IS NULL AND topic_id = t.id AND (user_id NOT IN (t.user_id, t.last_post_user_id)) GROUP BY user_id ORDER BY count_all DESC LIMIT 1 OFFSET 2) AS fu UNION (SELECT NULL) LIMIT 1),
           featured_user4_id     = (SELECT user_id FROM (SELECT COUNT(*) AS count_all, user_id FROM posts WHERE deleted_at IS NULL AND topic_id = t.id AND (user_id NOT IN (t.user_id, t.last_post_user_id)) GROUP BY user_id ORDER BY count_all DESC LIMIT 1 OFFSET 3) AS fu UNION (SELECT NULL) LIMIT 1)
  SQL

  Topic.exec_sql(sql)

  # TODO:
  # reply_count
  # incoming_link_count
  # excerpt
end

def update_posts_stats
  log "Updating posts stats..."

  sql = <<-SQL
    UPDATE posts p
    SET    reply_to_user_id = (
      SELECT u.id
      FROM   users u
      JOIN   posts p2 ON p2.user_id = u.id
                     AND p2.post_number = p.reply_to_post_number
                     AND p2.topic_id = p.topic_id
    )
  SQL

  Post.exec_sql(sql)
end

def update_search_data
  update_categories_search_data
  update_posts_search_data
  update_users_search_data
end

def update_categories_search_data
  log "Updating categories search data..."

  table_name = "category_search_data"
  Category.exec_sql("TRUNCATE #{table_name}")

  values = Category.select(:id, :name).to_a.map do |c|
    Category.sql_fragment("(:id, TO_TSVECTOR('#{Search.long_locale}', :search_data))", id: c.id, search_data: c.name)
  end.join(",")
  sql = "BEGIN; INSERT INTO #{table_name} (category_id, search_data) VALUES #{values}; COMMIT;"
  ActiveRecord::Base.connection.raw_connection.async_exec(sql)
end

def update_posts_search_data
  log "Updating posts search data..."

  table_name = "post_search_data"
  Post.exec_sql("TRUNCATE #{table_name}")

  Parallel.each(Topic.pluck(:id)) do |topic_id|
    @reconnected ||= ActiveRecord::Base.connection.reconnect! || true

    raw_search_data = Post.exec_sql("SELECT t.title AS topic_title, p.id AS post_id, p.cooked AS post_cooked, c.name AS category_name
                                     FROM topics t
                                     LEFT JOIN categories c ON c.id = t.category_id
                                     LEFT JOIN posts p ON t.id = p.topic_id
                                     WHERE t.id = :topic_id", topic_id: topic_id).to_a

    values = raw_search_data.map do |data|
      scrubed_search_data = [
        SearchObserver.scrub_html_for_search(data["post_cooked"]),
        data["topic_title"],
        data["category_name"],
      ].join(" ").strip

      Post.sql_fragment("(:id, TO_TSVECTOR('#{Search.long_locale}', :search_data))", id: data["post_id"], search_data: scrubed_search_data)
    end.join(",")

    sql = "BEGIN; INSERT INTO #{table_name} (post_id, search_data) VALUES #{values}; COMMIT;"
    ActiveRecord::Base.connection.raw_connection.async_exec(sql)
  end

  reconnect
end

def update_users_search_data
  log "Updating users search data..."

  table_name = "user_search_data"
  User.exec_sql("TRUNCATE #{table_name}")

  values = User.select(:id, :username, :name).to_a.map do |u|
    search_data = u.username << " " << (u.name || "")
    User.sql_fragment("(:id, TO_TSVECTOR('simple', :search_data))", id: u.id, search_data: search_data)
  end.join(",")
  sql = "BEGIN; INSERT INTO #{table_name} (user_id, search_data) VALUES #{values}; COMMIT;"
  ActiveRecord::Base.connection.raw_connection.async_exec(sql)
end

################################################################################

import_path = "<CHANGE ME>"

load_categories_mapping File.join(import_path, "categories.csv")
load_categories         File.join(import_path, "forum.csv")
load_categories_groups  File.join(import_path, "forumpermission.csv")

load_topics File.join(import_path, "thread.csv")
load_posts  File.join(import_path, "post.csv")

load_groups_mapping File.join(import_path, "groups.csv")
load_groups         File.join(import_path, "usergroup.csv")
load_users          File.join(import_path, "user.csv")

################################################################################

create_groups
create_users
create_groups_membership

create_categories
create_categories_groups

create_topics_and_posts

################################################################################

postprocess_posts

update_users_stats
update_groups_stats
update_topics_stats
update_posts_stats

update_search_data

log "DONE"
